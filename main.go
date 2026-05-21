// Package main demonstrates an infinite heartbeat workflow using the Resonate
// Go SDK.
//
// WHAT THIS DEMONSTRATES
//
// A single workflow — heartbeatWorkflow — runs a for loop with ctx.Sleep
// between iterations. In production the loop never exits: the workflow polls
// a service, fires an alert, sleeps, and repeats indefinitely without
// accumulating memory or history across ticks.
//
// WHY NO continueAsNew
//
// Temporal and similar systems impose an event-history cap (typically 50 000
// events). Long-running loops must periodically call continueAsNew to restart
// the workflow with fresh state — extracting accumulated state from the old
// execution and passing it forward as arguments is the developer's
// responsibility.
//
// Resonate has no event-history limit. Each ctx.Sleep creates a single durable
// timer promise. Once the promise settles, it is done — it does not accumulate
// in a growing replay log. The loop just runs. No continueAsNew needed.
//
// BOUNDED VS UNBOUNDED MODE
//
// The -iterations flag controls how many ticks the loop runs.
//
//	-iterations=0   Run forever (production mode).
//	-iterations=N   Exit after N ticks (default: 5, for CI and demos).
//
// HOW THIS EXAMPLE USES LOCALNET
//
// This example defaults to localnet — the in-process transport that ships with
// the Go SDK. No external server is required. Pass -url=<server> to run
// against a real Resonate server instead.
//
// Note: localnet stores server state in process memory. A process crash also
// destroys the timer state. For the full crash-recovery story (timer fires
// after the worker restarts), run against resonate dev.
//
// DURABLE EXECUTION AND REPLAY
//
// Resonate workflows are deterministic: when a workflow suspends on a pending
// promise and later resumes, the entire workflow body reruns from the top.
// Durable promises act as idempotency barriers — a promise that was already
// settled is short-circuited on replay; its stored result is returned
// immediately without re-executing the function.
//
// This means every tick call in the loop is itself a durable sub-invocation
// (via ctx.Run). On replay, completed ticks are skipped in microseconds; only
// the current tick actually executes.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"
)

// WorkflowArgs carries the loop parameters as plain integers so they
// round-trip cleanly through the durable promise codec (JSON).
// time.Duration marshals as a raw int64 nanosecond count; using int64 seconds
// avoids the unit ambiguity.
type WorkflowArgs struct {
	Iterations  int   `json:"iterations"`   // 0 = infinite
	IntervalSec int64 `json:"intervalSecs"` // sleep between ticks
}

// TickArgs is the argument struct for the heartbeat step.
type TickArgs struct {
	Tick       int    `json:"tick"`
	TotalLabel string `json:"totalLabel"` // "5" or "∞"
}

// heartbeatTick is a durable sub-step: it prints a single tick line and
// returns the timestamp. Wrapping side effects in ctx.Run makes them
// idempotent — on workflow replay, completed ticks are short-circuited by
// the promise store and don't re-execute.
func heartbeatTick(_ *resonate.Context, args TickArgs) (string, error) {
	ts := time.Now().UTC().Format(time.RFC3339)
	fmt.Printf("[workflow] tick %d/%s at %s\n", args.Tick, args.TotalLabel, ts)
	return ts, nil
}

// WorkflowResult is returned when the workflow exits (bounded mode only).
type WorkflowResult struct {
	Ticks    int    `json:"ticks"`
	Finished string `json:"finished"` // RFC3339 timestamp
}

// heartbeatWorkflow loops (args.Iterations times, or forever when
// args.Iterations == 0), printing a tick and sleeping between each iteration.
// Each sleep is a durable timer promise; each tick is a durable sub-step.
// Completed steps are not re-executed on replay.
func heartbeatWorkflow(ctx *resonate.Context, args WorkflowArgs) (WorkflowResult, error) {
	label := "∞"
	if args.Iterations > 0 {
		label = fmt.Sprintf("%d", args.Iterations)
	}

	for i := 0; args.Iterations == 0 || i < args.Iterations; i++ {
		tick := i + 1

		// Wrap the print in ctx.Run so it is a durable sub-step: the first
		// time this tick runs, the function executes and its result is stored.
		// On replay (after a sleep settles), the promise store returns the
		// cached result without re-executing — each tick line prints exactly once.
		tickF, err := ctx.Run(heartbeatTick, TickArgs{Tick: tick, TotalLabel: label})
		if err != nil {
			return WorkflowResult{}, fmt.Errorf("ctx.Run tick %d: %w", tick, err)
		}
		if err := tickF.Await(nil); err != nil {
			return WorkflowResult{}, fmt.Errorf("tick await %d: %w", tick, err)
		}

		// Sleep between ticks. ctx.Sleep returns a *Future; calling Await(nil)
		// blocks the workflow until the durable timer promise resolves.
		// When running against a real server, this suspension is crash-safe:
		// kill the worker, restart it, and the workflow resumes here after the
		// timer fires.
		sleepF, err := ctx.Sleep(time.Duration(args.IntervalSec) * time.Second)
		if err != nil {
			return WorkflowResult{}, fmt.Errorf("ctx.Sleep tick %d: %w", tick, err)
		}
		if err := sleepF.Await(nil); err != nil {
			return WorkflowResult{}, fmt.Errorf("sleep await tick %d: %w", tick, err)
		}
	}

	return WorkflowResult{
		Ticks:    args.Iterations,
		Finished: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func main() {
	iterations := flag.Int("iterations", 5, "number of loop iterations (0 = infinite)")
	intervalSecs := flag.Int64("interval-secs", 1, "seconds to sleep between ticks")
	serverURL := flag.String("url", "", "Resonate server URL (default: localnet)")
	flag.Parse()

	// Build a Resonate instance. Use localnet unless -url is specified.
	var cfg resonate.Config
	if *serverURL != "" {
		cfg = resonate.Config{
			URL: *serverURL,
		}
	} else {
		pid := "worker-1"
		cfg = resonate.Config{
			Network:   localnet.NewLocal("default", &pid),
			Heartbeat: resonate.NoopHeartbeat{},
		}
	}

	r, err := resonate.New(cfg)
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	// Register both functions. heartbeatTick is a sub-step called by
	// heartbeatWorkflow via ctx.Run; it must be registered so the runtime can
	// dispatch it durably.
	if _, err := resonate.Register(r, "heartbeatTick", heartbeatTick); err != nil {
		log.Fatalf("Register heartbeatTick: %v", err)
	}

	heartbeatFn, err := resonate.Register(r, "heartbeatWorkflow", heartbeatWorkflow)
	if err != nil {
		log.Fatalf("Register heartbeatWorkflow: %v", err)
	}

	ctx := context.Background()
	id := fmt.Sprintf("heartbeat-%d", time.Now().UnixNano())
	args := WorkflowArgs{
		Iterations:  *iterations,
		IntervalSec: *intervalSecs,
	}

	if *iterations == 0 {
		fmt.Printf("[main] starting infinite heartbeat workflow id=%s interval=%ds\n", id, *intervalSecs)
	} else {
		fmt.Printf("[main] starting heartbeat workflow id=%s iterations=%d interval=%ds\n", id, *iterations, *intervalSecs)
	}

	h, err := heartbeatFn.Run(ctx, id, args)
	if err != nil {
		log.Fatalf("Run: %v", err)
	}

	result, err := h.Result(ctx)
	if err != nil {
		log.Fatalf("Result: %v", err)
	}

	fmt.Printf("[main] done — %d ticks completed, finished at %s\n", result.Ticks, result.Finished)
}
