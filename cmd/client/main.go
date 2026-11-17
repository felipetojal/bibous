package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/felipetojal/bibous/internal/client"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("use put|get|delete|repair|status|scrub; consulte README.md")
	}
	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	nodesPath := flags.String("nodes", "nodes.json", "configuração dos três nós")
	manifest := flags.String("manifest", "", "manifesto JSON")
	input := flags.String("file", "", "arquivo para upload")
	output := flags.String("out", "", "arquivo de saída")
	nodeID := flags.String("node", "", "ID opcional para status/scrub")
	target := flags.String("target", "", "ID do nó para repair")
	timeout := flags.Duration("timeout", 10*time.Second, "limite por tentativa RPC")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 {
		return fmt.Errorf("argumentos inválidos")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	c := client.Client{Timeout: *timeout}
	switch command {
	case "put":
		if *input == "" || *manifest == "" {
			return fmt.Errorf("put exige -file e -manifest")
		}
		nodes, err := client.LoadNodes(*nodesPath)
		if err != nil {
			return err
		}
		if err = c.Upload(ctx, *input, *manifest, nodes); err != nil {
			return err
		}
	case "get":
		if *manifest == "" || *output == "" {
			return fmt.Errorf("get exige -manifest e -out")
		}
		if err := c.Download(ctx, *manifest, *output); err != nil {
			return err
		}
	case "delete":
		if *manifest == "" {
			return fmt.Errorf("delete exige -manifest")
		}
		if err := c.Delete(ctx, *manifest); err != nil {
			return err
		}
	case "repair":
		if *manifest == "" || *target == "" {
			return fmt.Errorf("repair exige -manifest e -target")
		}
		if err := c.Repair(ctx, *manifest, *target); err != nil {
			return err
		}
	case "status", "scrub":
		nodes, err := client.LoadNodes(*nodesPath)
		if err != nil {
			return err
		}
		var failures []error
		found := *nodeID == ""
		for _, n := range nodes {
			if *nodeID != "" && n.NodeID != *nodeID {
				continue
			}
			found = true
			var result any
			if command == "status" {
				result, err = c.Status(ctx, n)
			} else {
				result, err = c.Scrub(ctx, n)
			}
			if err != nil {
				failures = append(failures, fmt.Errorf("%s: %w", n.NodeID, err))
				continue
			}
			if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
				return err
			}
		}
		if !found {
			return fmt.Errorf("nó não consta em nodes.json: %s", *nodeID)
		}
		return errors.Join(failures...)
	default:
		return fmt.Errorf("comando desconhecido: %s", command)
	}
	fmt.Println(command + " concluído")
	return nil
}
