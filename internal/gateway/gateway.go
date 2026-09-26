// Package gateway exposes a Sim over HTTP: a live state stream, a
// command endpoint, and chaos-trigger endpoints for the dashboard.
package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/akshaydubey05/GoRedis/internal/sim"
)

type Gateway struct {
	sim           *sim.Sim
	allowedOrigin string
}

func New(s *sim.Sim, allowedOrigin string) *Gateway {
	return &Gateway{sim: s, allowedOrigin: allowedOrigin}
}

func (g *Gateway) cors(w http.ResponseWriter) {
	origin := g.allowedOrigin
	if origin == "" {
		origin = "*"
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func (g *Gateway) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/events", g.events)
	mux.HandleFunc("/api/command", g.command)
	mux.HandleFunc("/api/chaos/kill", g.chaosKill)
	mux.HandleFunc("/api/chaos/restart", g.chaosRestart)
	mux.HandleFunc("/api/chaos/partition", g.chaosPartition)
	mux.HandleFunc("/api/chaos/heal", g.chaosHeal)
	mux.HandleFunc("/api/reset", g.reset)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	return mux
}

// events streams cluster state to the browser every 300ms using
// Server-Sent Events — simple, one-way, auto-reconnecting in the
// browser, no extra library needed.
func (g *Gateway) events(w http.ResponseWriter, r *http.Request) {
	g.cors(w)
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			b, _ := json.Marshal(g.sim.Status())
			fmt.Fprintf(w, "event: state\ndata: %s\n\n", b)
			fl.Flush()
		}
	}
}

type commandReq struct {
	Cmd string `json:"cmd"`
}
type commandResp struct {
	Reply     string `json:"reply"`
	HandledBy int    `json:"handledBy"`
	Kind      string `json:"kind"`
}

func (g *Gateway) command(w http.ResponseWriter, r *http.Request) {
	g.cors(w)
	if r.Method == http.MethodOptions {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	var req commandReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	args := strings.Fields(req.Cmd)
	if len(args) == 0 {
		http.Error(w, "empty command", http.StatusBadRequest)
		return
	}
	if len(args) > 6 || len(req.Cmd) > 256 {
		http.Error(w, "command too long", http.StatusBadRequest)
		return
	}
	if !allowedCommand(strings.ToUpper(args[0])) {
		json.NewEncoder(w).Encode(commandResp{Reply: "ERR command not allowed in demo", Kind: "err", HandledBy: -1})
		return
	}

	reply, node := g.sim.Do(args)

	resp := commandResp{HandledBy: node, Kind: reply.Kind}
	switch reply.Kind {
	case "ok":
		resp.Reply = "OK"
	case "bulk":
		resp.Reply = reply.Str
	case "null":
		resp.Reply = "(nil)"
	case "int":
		resp.Reply = strconv.FormatInt(reply.Int, 10)
	case "err":
		resp.Reply = "ERR " + reply.Str
	case "array":
		resp.Reply = strings.Join(reply.Array, " ")
	}

	json.NewEncoder(w).Encode(resp)
}

func allowedCommand(cmd string) bool {
	switch cmd {
	case "SET", "GET", "DEL", "EXISTS", "INCR", "DECR", "INCRBY",
		"EXPIRE", "TTL", "HSET", "HGET", "HGETALL", "PING":
		return true
	}
	return false
}

func (g *Gateway) chaosKill(w http.ResponseWriter, r *http.Request) {
	g.cors(w)
	id, err := strconv.Atoi(r.URL.Query().Get("id"))
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	g.sim.Kill(id)
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gateway) chaosRestart(w http.ResponseWriter, r *http.Request) {
	g.cors(w)
	id, err := strconv.Atoi(r.URL.Query().Get("id"))
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	g.sim.Restart(id)
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gateway) chaosPartition(w http.ResponseWriter, r *http.Request) {
	g.cors(w)
	// Simple fixed demo partition: node 0 alone vs the rest.
	g.sim.Partition([][]int{{0}, {1, 2, 3, 4}})
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gateway) chaosHeal(w http.ResponseWriter, r *http.Request) {
	g.cors(w)
	g.sim.Heal()
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gateway) reset(w http.ResponseWriter, r *http.Request) {
	g.cors(w)
	g.sim.Reset()
	w.WriteHeader(http.StatusNoContent)
}