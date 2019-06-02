package main

import (
	"log"
	"os"

	"github.com/dotmesh-io/zumount/pkg/zumount"
)

// 0. run me in the host mount namespace
// 1. for a given zfs pool (first arg)
// 2. unmount all of the filesystems and snapshots for that zfs pool from all mount namespaces on the system

func main() {
	log.Print("Hello, zumount v1.2 starting")
	if len(os.Args) != 2 {
		log.Printf("specify pool to unmount")
		os.Exit(1)
	}
	err := zumount.Clean(os.Args[1])
	if err != nil {
		log.Printf("error cleaning: %s", err)
		os.Exit(1)
	}

}
