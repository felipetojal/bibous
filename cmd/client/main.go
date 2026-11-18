// Comando client: CLI do Bibous.
//
//	client [flags globais] <comando> [argumentos]
//
// Exemplos:
//
//	client -master localhost:8080 put ./foto.png /fotos/foto.png
//	client get /fotos/foto.png ./copia.png
//	client ls /fotos
//	client stat /fotos/foto.png
//	client rm /fotos/foto.png
//	client mv /fotos/foto.png /fotos/2026/foto.png
//	client mkdir -p /fotos/2026
//	client rmdir -r /fotos
//	client node-test -addr localhost:8081
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/felipetojal/bibous/internal/client"
	pb "github.com/felipetojal/bibous/proto"
)

const usage = `uso: client [flags globais] <comando> [argumentos]

Comandos:
  put [-f] <arquivo local> [caminho remoto]   envia um arquivo
  get <caminho remoto> [destino local]        baixa um arquivo
  ls [diretório]                              lista arquivos e subdiretórios
  stat <caminho remoto>                       metadados, estado e réplicas dos blocos
  rm <caminho remoto>                         remove um arquivo
  mv [-f] <origem> <destino>                  renomeia/move arquivo ou diretório
  mkdir [-p] <diretório>                      cria diretório
  rmdir [-r] <diretório>                      remove diretório (vazio, ou tudo com -r)
  node-test [-addr host:porta]                testa um nó de storage direto (sem mestre)

Flags globais:
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		os.Exit(1)
	}
}

func run() error {
	defaultMaster := os.Getenv("BIBOUS_MASTER")
	if defaultMaster == "" {
		defaultMaster = "localhost:8080"
	}

	global := flag.NewFlagSet("client", flag.ExitOnError)
	master := global.String("master", defaultMaster, "endereço do mestre (ou env BIBOUS_MASTER)")
	blockSize := global.Int64("block-size", client.DefaultBlockSize, "tamanho do bloco em bytes (máx. 4 MiB)")
	replication := global.Uint("replicas", 0, "fator de replicação pedido ao mestre (0 = padrão do mestre)")
	minReplicas := global.Int("min-replicas", 1, "réplicas que precisam confirmar cada bloco")
	parallel := global.Int("parallel", 4, "blocos transferidos em paralelo")
	timeout := global.Duration("timeout", 30*time.Second, "timeout de cada chamada RPC")
	retries := global.Int("retries", 2, "novas tentativas por nó em falhas transitórias")
	quiet := global.Bool("q", false, "não mostrar progresso")
	global.Usage = func() {
		fmt.Fprint(global.Output(), usage)
		global.PrintDefaults()
	}
	global.Parse(os.Args[1:])

	args := global.Args()
	if len(args) == 0 {
		global.Usage()
		os.Exit(2)
	}
	cmd, args := args[0], args[1:]

	// node-test não usa o mestre.
	if cmd == "node-test" {
		return runNodeTest(args)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := client.Config{
		MasterAddr:        *master,
		BlockSize:         *blockSize,
		ReplicationFactor: uint32(*replication),
		Parallelism:       *parallel,
		MinReplicas:       *minReplicas,
		RPCTimeout:        *timeout,
		Retries:           *retries,
	}
	if !*quiet {
		cfg.Progress = func(done, total int) {
			fmt.Fprintf(os.Stderr, "\r  blocos: %d/%d", done, total)
			if done == total {
				fmt.Fprintln(os.Stderr)
			}
		}
	}

	c, err := client.New(cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	switch cmd {
	case "put":
		return cmdPut(ctx, c, args)
	case "get":
		return cmdGet(ctx, c, args)
	case "ls":
		return cmdList(ctx, c, args)
	case "stat":
		return cmdStat(ctx, c, args)
	case "rm":
		return cmdRemove(ctx, c, args)
	case "mv":
		return cmdMove(ctx, c, args)
	case "mkdir":
		return cmdMakeDir(ctx, c, args)
	case "rmdir":
		return cmdRemoveDir(ctx, c, args)
	default:
		global.Usage()
		return fmt.Errorf("comando desconhecido: %q", cmd)
	}
}

func cmdPut(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	overwrite := fs.Bool("f", false, "sobrescrever se o arquivo remoto já existir")
	fs.Parse(args)

	if fs.NArg() < 1 || fs.NArg() > 2 {
		return errors.New("uso: put [-f] <arquivo local> [caminho remoto]")
	}
	local := fs.Arg(0)
	remote := fs.Arg(1)
	if remote == "" {
		remote = "/" + filepath.Base(local)
	}

	start := time.Now()
	info, err := c.Upload(ctx, local, remote, client.UploadOptions{Overwrite: *overwrite})
	if err != nil {
		return err
	}
	fmt.Printf("enviado %s -> %s (%s, %d blocos) em %s\n",
		local, info.GetPath(), humanSize(info.GetSize()), info.GetBlockCount(),
		time.Since(start).Round(time.Millisecond))
	return nil
}

func cmdGet(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("uso: get <caminho remoto> [destino local]")
	}
	remote := args[0]
	local := path.Base(remote)
	if len(args) == 2 {
		local = args[1]
	}
	if st, err := os.Stat(local); err == nil && st.IsDir() {
		local = filepath.Join(local, path.Base(remote))
	}

	start := time.Now()
	info, err := c.Download(ctx, remote, local)
	if err != nil {
		return err
	}
	fmt.Printf("baixado %s -> %s (%s) em %s\n",
		info.GetPath(), local, humanSize(info.GetSize()),
		time.Since(start).Round(time.Millisecond))
	return nil
}

func cmdList(ctx context.Context, c *client.Client, args []string) error {
	prefix := ""
	if len(args) > 0 {
		prefix = args[0]
	}

	files, err := c.List(ctx, prefix)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TIPO\tTAMANHO\tRÉPLICAS\tESTADO\tMODIFICADO\tCAMINHO")
	for _, f := range files {
		if f.GetIsDir() {
			fmt.Fprintf(w, "d\t-\t-\t-\t%s\t%s/\n",
				formatUnix(f.GetUpdatedAtUnix()), strings.TrimSuffix(f.GetPath(), "/"))
			continue
		}
		fmt.Fprintf(w, "-\t%s\t%d\t%s\t%s\t%s\n",
			humanSize(f.GetSize()), f.GetReplicationFactor(), stateName(f.GetState()),
			formatUnix(f.GetUpdatedAtUnix()), f.GetPath())
	}
	return w.Flush()
}

func cmdStat(ctx context.Context, c *client.Client, args []string) error {
	if len(args) != 1 {
		return errors.New("uso: stat <caminho remoto>")
	}

	resp, err := c.Stat(ctx, args[0])
	if err != nil {
		return err
	}
	printStat(resp)
	return nil
}

func cmdRemove(ctx context.Context, c *client.Client, args []string) error {
	if len(args) != 1 {
		return errors.New("uso: rm <caminho remoto>")
	}
	if err := c.Delete(ctx, args[0]); err != nil {
		return err
	}
	fmt.Printf("removido %s\n", args[0])
	return nil
}

func cmdMove(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("mv", flag.ExitOnError)
	overwrite := fs.Bool("f", false, "sobrescrever o destino se existir")
	fs.Parse(args)
	if fs.NArg() != 2 {
		return errors.New("uso: mv [-f] <origem> <destino>")
	}

	info, err := c.Move(ctx, fs.Arg(0), fs.Arg(1), *overwrite)
	if err != nil {
		return err
	}
	dest := fs.Arg(1)
	if info.GetPath() != "" {
		dest = info.GetPath()
	}
	fmt.Printf("movido %s -> %s\n", fs.Arg(0), dest)
	return nil
}

func cmdMakeDir(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("mkdir", flag.ExitOnError)
	parents := fs.Bool("p", false, "criar diretórios intermediários")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("uso: mkdir [-p] <diretório>")
	}

	if err := c.MakeDir(ctx, fs.Arg(0), *parents); err != nil {
		return err
	}
	fmt.Printf("diretório criado %s\n", fs.Arg(0))
	return nil
}

func cmdRemoveDir(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("rmdir", flag.ExitOnError)
	recursive := fs.Bool("r", false, "remover também o conteúdo")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("uso: rmdir [-r] <diretório>")
	}

	if err := c.RemoveDir(ctx, fs.Arg(0), *recursive); err != nil {
		return err
	}
	fmt.Printf("diretório removido %s\n", fs.Arg(0))
	return nil
}

func stateName(s pb.FileState) string {
	switch s {
	case pb.FileState_FILE_STATE_HEALTHY:
		return "íntegro"
	case pb.FileState_FILE_STATE_UNDER_REPLICATED:
		return "sub-replicado"
	case pb.FileState_FILE_STATE_RECOVERING:
		return "em recuperação"
	}
	return "desconhecido"
}

func printStat(resp *pb.GetFileResponse) {
	f := resp.GetFile()
	fmt.Printf("caminho:     %s\n", f.GetPath())
	fmt.Printf("tamanho:     %s (%d bytes)\n", humanSize(f.GetSize()), f.GetSize())
	fmt.Printf("sha256:      %s\n", f.GetHash())
	fmt.Printf("estado:      %s\n", stateName(f.GetState()))
	fmt.Printf("replicação:  %d\n", f.GetReplicationFactor())
	fmt.Printf("criado:      %s\n", formatUnix(f.GetCreatedAtUnix()))
	fmt.Printf("modificado:  %s\n", formatUnix(f.GetUpdatedAtUnix()))
	fmt.Printf("blocos:      %d\n", len(resp.GetBlocks()))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  #\tBLOCO\tVERSÃO\tTAMANHO\tRÉPLICAS")
	for _, b := range resp.GetBlocks() {
		nodes := make([]string, 0, len(b.GetReplicas()))
		for _, r := range b.GetReplicas() {
			nodes = append(nodes, r.GetNodeId()+"@"+r.GetAddress())
		}
		fmt.Fprintf(w, "  %d\t%s\t%d\t%s\t%s\n",
			b.GetIndex(), b.GetBlockId(), b.GetVersion(),
			humanSize(b.GetSize()), strings.Join(nodes, ", "))
	}
	w.Flush()
}

func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func formatUnix(sec int64) string {
	if sec == 0 {
		return "-"
	}
	return time.Unix(sec, 0).Format("2006-01-02 15:04")
}
