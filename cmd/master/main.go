package main

import (
	"log"

	"github.com/Sanim27/dfs/internal/master"
)

func main() {
	m := master.NewMaster(":9000")

	log.Println("[MASTER] starting on :9000 ...")
	if err := m.Start(); err != nil {
		log.Fatal(err)
	}

	select {} // keep running
}
