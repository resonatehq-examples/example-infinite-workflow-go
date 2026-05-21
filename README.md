# Infinite Workflow | Resonate Go SDK

A heartbeat workflow that runs forever — sleeping, printing a tick, sleeping, repeating — without accumulating memory or history across iterations.

> Heads up — `resonate-sdk-go` is pre-release. The SDK has no semver tag yet, so this example pins to a specific commit. Expect API changes until `v0.1.0`.

## What this demonstrates

- **Unbounded execution**: the workflow loops indefinitely with no history limit.
- **Durable sleep**: `ctx.Sleep()` between iterations survives crashes and restarts.
- **No continueAsNew**: Resonate has no event-history cap — just loop.

## Why no continueAsNew

Temporal and similar systems impose an event-history limit (typically 50 000 events). A long-running loop must periodically call `continueAsNew` to restart the workflow with fresh state, extracting accumulated state from the old execution and passing it forward as arguments. That is the developer's problem.

Resonate has no event-history limit. Each `ctx.Sleep` creates a single durable timer promise. Once the promise settles it is done — it does not accumulate in a growing replay log. The loop just runs. No `continueAsNew` required.

## The code

```go
func heartbeatWorkflow(ctx *resonate.Context, args WorkflowArgs) (WorkflowResult, error) {
    for i := 0; args.Iterations == 0 || i < args.Iterations; i++ {
        fmt.Printf("[workflow] tick %d at %s\n", i+1, time.Now().UTC().Format(time.RFC3339))

        f, err := ctx.Sleep(time.Duration(args.IntervalSec) * time.Second)
        if err != nil {
            return WorkflowResult{}, fmt.Errorf("ctx.Sleep: %w", err)
        }
        if err := f.Await(nil); err != nil {
            return WorkflowResult{}, fmt.Errorf("sleep await: %w", err)
        }
    }
    return WorkflowResult{Ticks: args.Iterations, Finished: time.Now().UTC().Format(time.RFC3339)}, nil
}
```

`ctx.Sleep` returns `(*Future, error)`. Calling `f.Await(nil)` blocks the workflow until the durable timer promise resolves. Each settled promise is discarded — no history accumulation.

## Bounded vs unbounded mode

| Flag | Behaviour |
|------|-----------|
| `-iterations=5` (default) | Exit after 5 ticks — CI-friendly, good for demos |
| `-iterations=0` | Loop forever — production mode |

In production you would set `-iterations=0` (or remove the bound from the loop condition entirely) and let the workflow run until you explicitly cancel it.

## Prerequisites

- Go 1.22+
- The `resonate` server CLI (required only if you pass `-url`). Install with Homebrew on macOS or Linux:
  ```
  brew install resonatehq/tap/resonate
  ```
  Other install paths: <https://docs.resonatehq.io/get-started/install>.

By default the example uses `localnet` — an in-process transport that requires no external server.

## Setup

```sh
git clone https://github.com/resonatehq-examples/example-infinite-workflow-go.git
cd example-infinite-workflow-go
go mod download
```

## Run it

**Default (5 iterations, localnet):**

```sh
go run .
```

**Custom iteration count and interval:**

```sh
go run . -iterations=10 -interval-secs=2
```

**Infinite mode against a real server:**

```sh
resonate dev          # in another terminal
go run . -iterations=0 -url=http://localhost:8001
```

## What to look for

```
[main] starting heartbeat workflow id=heartbeat-... iterations=5 interval=1s
[workflow] tick 1/5 at 2026-05-21T15:00:00Z
[workflow] tick 2/5 at 2026-05-21T15:00:01Z
[workflow] tick 3/5 at 2026-05-21T15:00:02Z
[workflow] tick 4/5 at 2026-05-21T15:00:03Z
[workflow] tick 5/5 at 2026-05-21T15:00:04Z
[main] done — 5 ticks completed, finished at 2026-05-21T15:00:05Z
```

Each tick is separated by a durable sleep. When running against a real server, killing and restarting the worker between ticks will resume from the last completed tick — the timer promise that was already created on the server fires normally, and the workflow picks up where it left off.

## Localnet note

The default transport (`localnet`) stores server state in process memory. A process crash also destroys timer state. For the full crash-recovery demo, run against `resonate dev` with `-url=http://localhost:8001`.

## File structure

```
example-infinite-workflow-go/
├── main.go        workflow and entry point
├── go.mod         module declaration + SDK pin
├── go.sum         checksums
├── LICENSE        Apache-2.0
└── README.md
```

## Next steps

- [Get started](https://docs.resonatehq.io/get-started) — install paths + first-program walkthrough.
- [Durable execution concepts](https://docs.resonatehq.io/concepts) — what makes invocations durable + how the runtime resumes them.
- [`example-hello-world-go`](https://github.com/resonatehq-examples/example-hello-world-go) — the simplest possible Resonate program in Go.

## Community

- Discord: <https://resonatehq.io/discord>
- X: <https://x.com/resonatehqio>
- LinkedIn: <https://linkedin.com/company/resonatehq>
- YouTube: <https://youtube.com/@resonatehq>
- Journal: <https://journal.resonatehq.io>

## License

[Apache-2.0](./LICENSE)
