package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/felipetojal/bibous/internal/storage"
	"github.com/joho/godotenv"
)


func main(){

	if err := godotenv.Load(".env"); err != nil {
		log.Fatalf("carragar .env: %v", err)
	}

	cfg, err := storage.LoadConfig()

	if err != nil {
		log.Fatal(err)
	}

	str, err := storage.NewStore(cfg.DataDir)

	if err != nil {
		log.Fatal(err)
	}

	bt := []byte("first block")
  blk, err := storage.CreateBlock("B1", 1, bt)

	if err != nil {
		log.Fatal(err)
	}

	if err := str.SaveBlock(blk, bt); err != nil {
		log.Fatal(err)
	}
	
	if blk.State != storage.VALID {
		fmt.Printf("Estado incorreto: %v", blk.State)
	}

	fmt.Printf("Blk salvo %s", blk.BlockID)

	byt, err := str.ReadBlock(blk)

	if err != nil {
		log.Fatal(err)
	}

	if !bytes.Equal(bt, byt) {
		fmt.Printf("bytes diferentes")
	}
	
	fmt.Printf("Tudo ok")

}
