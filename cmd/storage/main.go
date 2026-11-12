package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	storage "sistemasDistribuidos/bibous/internal/storage/config"
)

func PrepareDataDir(dir string) (err error) {
	if err := os.MkdirAll(dir,0750); err != nil {
		return fmt.Errorf("criar diretorio")
	}

	f , err := os.CreateTemp(dir, "write-check")
	if err != nil {
		return fmt.Errorf("Criar temporario")
	}

	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("fechar temporario"))
		}

		if removeErr := os.Remove(f.Name()); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remover temporario"))
		}
	}()

	if _, err := f.Write([]byte("write-check")); err != nil {
		return fmt.Errorf("escrever temporario")
	}

	return nil
}

func main(){
	cfg, err := storage.LoadConfig()

	if err != nil {
		log.Fatal(err)
	}

	if err := PrepareDataDir(cfg.DataDir); err != nil {
		log.Fatal(err)
	}

}
