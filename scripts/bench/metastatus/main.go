// Command metastatus prints one meta replica's own view of raft, for shell
// harnesses that need to see a replica rejoin rather than infer it from the
// data path. A gate follows leader redirects, so it keeps serving from a
// remote leader while its local replica is still out of the cluster: asking
// the replica directly is the only way to tell those apart.
//
// Each node is reported on its own line as key=value pairs, and a node that
// cannot be reached is reported as an error line rather than aborting the
// run: "did not answer" is an observation the caller is polling for.
//
//	go run ./scripts/bench/metastatus -config c.toml -ca server.pem 2 5 8 11
//
// The exit status covers reachability alone — 0 when every node answered, 1
// when any did not. What the answers say is left to the caller.
package main

import (
	"context"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/mulgadc/predastore"
)

func main() {
	configPath := flag.String("config", "", "cluster config file")
	caPath := flag.String("ca", "", "PEM holding the cluster's trust anchor")
	timeout := flag.Duration("timeout", 5*time.Second, "per-node deadline")
	flag.Parse()

	if *configPath == "" || *caPath == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: metastatus -config <file> -ca <file> <node id>...")
		os.Exit(2)
	}

	cfg, err := predastore.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(2)
	}

	// The cluster's certificate is self-signed and is its own anchor, so the
	// pool holds that one file rather than a chain.
	pem, err := os.ReadFile(*caPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read ca: %v\n", err)
		os.Exit(2)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		fmt.Fprintf(os.Stderr, "no certificate in %s\n", *caPath)
		os.Exit(2)
	}

	failed := false
	for _, arg := range flag.Args() {
		id, err := strconv.ParseUint(arg, 10, 32)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad node id %q: %v\n", arg, err)
			os.Exit(2)
		}
		node := predastore.NodeID(id)

		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		status, err := predastore.NodeStatus(ctx, cfg, node, roots)
		cancel()
		if err != nil {
			fmt.Printf("node=%d error=%q\n", node, err)
			failed = true
			continue
		}

		fmt.Printf("node=%s state=%s leader=%q term=%s commit=%s applied=%s is_leader=%t\n",
			status.NodeID, status.State, status.Leader,
			status.Term, status.CommitIndex, status.AppliedIndex, status.IsLeader)
	}

	if failed {
		os.Exit(1)
	}
}
