// Comando client: CLI do Bibous.
//
//	client [flags globais] <comando> [argumentos]
//
// Exemplos:
//
//	client -master localhost:50051 mkdir -p /fotos
//	client put ./foto.png /fotos/foto.png
//	client get /fotos/foto.png ./copia.png
//	client ls /fotos
//	client stat /fotos/foto.png
//	client mv /fotos/foto.png /fotos/praia.png
//	client rm /fotos/praia.png
//	client rmdir -r /fotos
//	client status
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
)

const usage = `uso: client [flags globais] <comando> [argumentos]

Arquivos:
  put [-f] <arquivo local> [caminho remoto]   envia um arquivo (-f sobrescreve)
  get <caminho remoto> [destino local]        baixa um arquivo
  stat <caminho remoto>                       tamanho, data, hash, estado e réplicas
  rm <caminho remoto>                         remove um arquivo (e os blocos nos nós)
  mv [-f] <origem> <destino>                  renomeia/move um arquivo

Diretórios:
  ls [diretório]                              lista o conteúdo (padrão: /)
  mkdir [-p] <diretório>                      cria diretório
  rmdir [-r] <diretório>                      remove diretório (vazio, ou tudo com -r)

Cluster:
  status                                      nós (vivos/suspeitos/mortos), espaço e arquivos por estado
  drain <node_id>                             esvazia um nó antes de desligá-lo
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
		defaultMaster = "localhost:50051"
	}

	global := flag.NewFlagSet("client", flag.ExitOnError)
	master := global.String("master", defaultMaster, "endereço do mestre (ou env BIBOUS_MASTER)")
	blockSize := global.Int64("block-size", 0, "tamanho do bloco em bytes (padrão e máximo: 4 MiB)")
	replication := global.Uint("replicas", 0, "fator de replicação pedido ao mestre (0 = padrão do mestre)")
	parallel := global.Int("parallel", 4, "blocos transferidos em paralelo")
	timeout := global.Duration("timeout", 30*time.Second, "timeout de cada chamada RPC")
	retries := global.Int("retries", 2, "novas tentativas por nó em falhas transitórias")
	quiet := global.Bool("q", false, "não mostrar progresso")
	key := global.String("key", os.Getenv("BIBOUS_KEY"), "chave AES-256 de 32 caracteres (ou env BIBOUS_KEY); vazia = sem criptografia")
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
		RPCTimeout:        *timeout,
		Retries:           *retries,
		EncryptionKey:     []byte(*key),
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
	case "status":
		return cmdStatus(ctx, c)
	case "drain":
		return cmdDrain(ctx, c, args)
	default:
		global.Usage()
		return fmt.Errorf("comando desconhecido: %q", cmd)
	}
}

// parse aceita flags antes ou depois dos argumentos posicionais
// (ex.: "put arquivo /x -f" e "put -f arquivo /x").
func parse(fs *flag.FlagSet, args []string) []string {
	var positional []string
	for {
		fs.Parse(args)
		args = fs.Args()
		if len(args) == 0 {
			return positional
		}
		positional = append(positional, args[0])
		args = args[1:]
	}
}

func cmdPut(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	overwrite := fs.Bool("f", false, "sobrescrever se o arquivo remoto já existir")
	pos := parse(fs, args)
	if len(pos) < 1 || len(pos) > 2 {
		return errors.New("uso: put [-f] <arquivo local> [caminho remoto]")
	}
	local := pos[0]
	remote := "/" + filepath.Base(local)
	if len(pos) == 2 {
		remote = pos[1]
	}
	// "put foto.png /fotos/" envia para dentro do diretório.
	if strings.HasSuffix(remote, "/") {
		remote += filepath.Base(local)
	}

	start := time.Now()
	info, err := c.Upload(ctx, local, remote, client.UploadOptions{Overwrite: *overwrite})
	if err != nil {
		return err
	}
	fmt.Printf("enviado %s -> %s (%s, %d blocos, %d réplicas) em %s\n",
		local, info.GetPath(), humanSize(int64(info.GetSize())), len(info.GetBlocks()),
		info.GetReplicationFactor(), time.Since(start).Round(time.Millisecond))
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
	fmt.Printf("baixado %s -> %s (%s) em %s, hash conferido\n",
		info.GetPath(), local, humanSize(int64(info.GetSize())), time.Since(start).Round(time.Millisecond))
	return nil
}

func cmdList(ctx context.Context, c *client.Client, args []string) error {
	dir := "/"
	if len(args) > 0 {
		dir = args[0]
	}
	entries, err := c.List(ctx, dir)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TIPO\tTAMANHO\tRÉPLICAS\tESTADO\tMODIFICADO\tCAMINHO")
	for _, e := range entries {
		if e.IsDir {
			fmt.Fprintf(w, "d\t-\t-\t-\t-\t%s/\n", e.Path)
			continue
		}
		f := e.File
		fmt.Fprintf(w, "-\t%s\t%d\t%s\t%s\t%s\n",
			humanSize(int64(f.GetSize())), f.GetReplicationFactor(), stateName(f.GetStatus()),
			formatUnix(f.GetUpdatedUnix()), e.Path)
	}
	return w.Flush()
}

func cmdStat(ctx context.Context, c *client.Client, args []string) error {
	if len(args) != 1 {
		return errors.New("uso: stat <caminho remoto>")
	}
	f, err := c.Stat(ctx, args[0])
	if err != nil {
		return err
	}

	fmt.Printf("caminho:     %s\n", f.GetPath())
	fmt.Printf("tamanho:     %s (%d bytes armazenados)\n", humanSize(int64(f.GetSize())), f.GetSize())
	fmt.Printf("sha256:      %s\n", f.GetSha256())
	fmt.Printf("estado:      %s\n", stateName(f.GetStatus()))
	fmt.Printf("replicação:  %d\n", f.GetReplicationFactor())
	fmt.Printf("criado:      %s\n", formatUnix(f.GetCreatedUnix()))
	fmt.Printf("modificado:  %s\n", formatUnix(f.GetUpdatedUnix()))
	fmt.Printf("blocos:      %d\n", len(f.GetBlocks()))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  #\tBLOCO\tVERSÃO\tTAMANHO\tRÉPLICAS")
	for i, b := range f.GetBlocks() {
		nodes := make([]string, 0, len(b.GetReplicas()))
		for _, r := range b.GetReplicas() {
			nodes = append(nodes, r.GetNodeId()+"@"+r.GetAddress())
		}
		fmt.Fprintf(w, "  %d\t%s\t%d\t%s\t%s\n",
			i, b.GetBlockId(), b.GetVersion(), humanSize(int64(b.GetSize())), strings.Join(nodes, ", "))
	}
	return w.Flush()
}

func cmdRemove(ctx context.Context, c *client.Client, args []string) error {
	if len(args) != 1 {
		return errors.New("uso: rm <caminho remoto>")
	}
	n, err := c.DeleteVerbose(ctx, args[0])
	if err != nil {
		return err
	}
	fmt.Printf("removido %s (%d cópias de blocos apagadas nos nós)\n", args[0], n)
	return nil
}

func cmdMove(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("mv", flag.ExitOnError)
	overwrite := fs.Bool("f", false, "sobrescrever o destino se existir")
	pos := parse(fs, args)
	if len(pos) != 2 {
		return errors.New("uso: mv [-f] <origem> <destino>")
	}
	if err := c.Move(ctx, pos[0], pos[1], *overwrite); err != nil {
		return err
	}
	fmt.Printf("movido %s -> %s\n", pos[0], pos[1])
	return nil
}

func cmdMakeDir(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("mkdir", flag.ExitOnError)
	parents := fs.Bool("p", false, "criar diretórios intermediários")
	pos := parse(fs, args)
	if len(pos) != 1 {
		return errors.New("uso: mkdir [-p] <diretório>")
	}
	if err := c.MakeDir(ctx, pos[0], *parents); err != nil {
		return err
	}
	fmt.Printf("diretório criado %s\n", pos[0])
	return nil
}

func cmdRemoveDir(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("rmdir", flag.ExitOnError)
	recursive := fs.Bool("r", false, "remover também o conteúdo")
	pos := parse(fs, args)
	if len(pos) != 1 {
		return errors.New("uso: rmdir [-r] <diretório>")
	}
	if err := c.RemoveDir(ctx, pos[0], *recursive); err != nil {
		return err
	}
	fmt.Printf("diretório removido %s\n", pos[0])
	return nil
}

func cmdStatus(ctx context.Context, c *client.Client) error {
	st, err := c.ClusterStatus(ctx)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NÓ\tENDEREÇO\tMÁQUINA/DISCO\tESTADO\tLIVRE\tTOTAL\tÚLTIMO HEARTBEAT")
	for _, n := range st.GetNodes() {
		ago := "-"
		if n.GetLastHeartbeatUnix() > 0 {
			ago = time.Since(time.Unix(n.GetLastHeartbeatUnix(), 0)).Round(time.Second).String() + " atrás"
		}
		fmt.Fprintf(w, "%s\t%s\t%s/%s\t%s\t%s\t%s\t%s\n",
			n.GetNodeId(), n.GetAddress(), n.GetMachineId(), n.GetDiskId(), nodeStateName(n.GetStatus()),
			humanSize(int64(n.GetAvailableSize())), humanSize(int64(n.GetTotalSize())), ago)
	}
	w.Flush()

	fmt.Printf("\narquivos: %d íntegros, %d sub-replicados, %d indisponíveis; %d uploads pendentes\n",
		st.GetHealthyFiles(), st.GetUnderReplicatedFiles(), st.GetUnavailableFiles(), st.GetPendingUploads())
	return nil
}

func cmdDrain(ctx context.Context, c *client.Client, args []string) error {
	if len(args) != 1 {
		return errors.New("uso: drain <node_id>")
	}
	if err := c.Drain(ctx, args[0]); err != nil {
		return err
	}
	fmt.Printf("nó %s em drenagem: não recebe novas réplicas e o mestre copiará seus blocos\n", args[0])
	return nil
}

func stateName(s string) string {
	switch s {
	case "HEALTHY":
		return "íntegro"
	case "UNDER_REPLICATED":
		return "sub-replicado"
	case "UNAVAILABLE":
		return "indisponível"
	case "":
		return "-"
	}
	return strings.ToLower(s)
}

func nodeStateName(s string) string {
	switch s {
	case "ALIVE":
		return "vivo"
	case "SUSPECT":
		return "suspeito"
	case "DEAD":
		return "morto"
	case "DRAINING":
		return "drenando"
	}
	return s
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
	if sec <= 0 {
		return "-"
	}
	return time.Unix(sec, 0).Format("2006-01-02 15:04")
}
