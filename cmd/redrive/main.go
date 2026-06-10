package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	os.Exit(realMain())
}

// defaultBrokers is the --brokers default: the KAFKA_BROKERS environment
// variable (the same variable every service reads via kafka.Config), falling
// back to the local dev listener.
func defaultBrokers() string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return v
	}
	return "localhost:9092"
}

// realMain runs the CLI and returns the process exit code; keeping os.Exit
// out of the deferring function so cleanup (signal stop) always runs.
func realMain() int {
	var (
		brokers  = flag.String("brokers", defaultBrokers(), "comma-separated Kafka bootstrap brokers (default: KAFKA_BROKERS env, then localhost:9092)")
		dlt      = flag.String("dlt", "", "dead-letter topic to drain (required), e.g. orders.commands.DLT")
		limit    = flag.Int("limit", 0, "max records to process (0 = all pending)")
		dryRun   = flag.Bool("dry-run", false, "list pending records without republishing or committing")
		freshIDs = flag.Bool("fresh-ids", false, "mint new message-id headers (bypass inbox dedup — projection rebuild mode)")
		group    = flag.String("group", "redrive", "consumer group used to track redrive progress")
	)
	flag.Parse()

	if *dlt == "" {
		fmt.Fprintln(os.Stderr, "redrive: --dlt is required")
		flag.Usage()
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stats, err := Run(ctx, Config{
		Brokers:  strings.Split(*brokers, ","),
		DLT:      *dlt,
		Limit:    *limit,
		DryRun:   *dryRun,
		FreshIDs: *freshIDs,
		Group:    *group,
		Out:      os.Stdout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "redrive: %v (read %d, republished %d)\n", err, stats.Read, stats.Republished)
		return 1
	}
	return 0
}
