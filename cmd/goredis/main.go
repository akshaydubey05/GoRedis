package main

import (
	"fmt"
	"os"

	"github.com/akshaydubey05/GoRedis/internal/aof"
	"github.com/akshaydubey05/GoRedis/internal/server"
	"github.com/akshaydubey05/GoRedis/internal/store"
)

func main() {
	db := store.New()

	a, err := aof.New("database.aof")
	if err != nil {
		fmt.Println("aof error:", err)
		os.Exit(1)
	}
	defer a.Close()

	srv := server.New(db, a)

	// Replay past commands into the store before accepting any
	// client connections, same order as your original main.go.
	if err := a.Read(srv.ApplyFromAOF); err != nil {
		fmt.Println("aof replay error:", err)
		os.Exit(1)
	}

	if err := srv.Listen(":6379"); err != nil {
		fmt.Println("listen error:", err)
		os.Exit(1)
	}
}