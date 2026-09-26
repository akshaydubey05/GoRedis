package main

import (
	"log"
	"net/http"
	"os"

	"github.com/akshaydubey05/GoRedis/internal/gateway"
	"github.com/akshaydubey05/GoRedis/internal/sim"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	origin := os.Getenv("ALLOWED_ORIGIN") // e.g. https://your-frontend.vercel.app

	s := sim.New(3)
	g := gateway.New(s, origin)

	log.Println("gateway listening on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, g.Routes()))
}