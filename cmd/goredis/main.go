package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/akshaydubey05/GoRedis/internal/aof"
	"github.com/akshaydubey05/GoRedis/internal/resp"
	"github.com/akshaydubey05/GoRedis/internal/server"
	"github.com/akshaydubey05/GoRedis/internal/store"
)

func main() {
	fmt.Println("Listening on port :6379")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	archive, err := aof.New("database.aof")
	if err != nil {
		log.Fatal(err)
	}
	defer archive.Close()

	st := store.New()
	srv := server.New(st, archive)

	if err := archive.Replay(func(value resp.Value) error {
		_ = srv.Execute(value, false)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			st.DeleteExpired(time.Now().UnixMilli())
		}
	}()

	if err := srv.Serve(l); err != nil {
		log.Fatal(err)
	}
}
