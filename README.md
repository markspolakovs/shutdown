# shutdown

`shutdown` is a small Go package for coordinating graceful application shutdown.

It listens for `SIGTERM` and `SIGINT`, cancels shutdown-aware contexts, runs registered cleanup handlers, waits for them to finish, and then exits the process.

## Install

```sh
go get github.com/markspolakovs/shutdown
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/markspolakovs/shutdown"
)

func main() {
	shutdown.Init(shutdown.Options{
		GracePeriod: 30 * time.Second,
	})

	server := &http.Server{Addr: ":8080"}

	shutdown.Handle(func(ctx context.Context) {
		_ = server.Shutdown(ctx)
	})

	go func() {
		<-shutdown.Ctx(context.Background()).Done()
		fmt.Println("shutdown requested")
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}
```

## How it works

Call `shutdown.Init` once near the start of your program. By default it installs a global signal handler for `SIGTERM` and `SIGINT`.

When shutdown starts, the package:

1. Cancels every context created with `shutdown.Ctx`.
2. Runs every function registered with `shutdown.Handle` concurrently.
3. Gives handlers a deadline derived from the configured grace period.
4. Waits for all handlers to return.
5. Exits the process with status code `0`.

The handler deadline is 80% of the configured grace period, leaving a small buffer before an external supervisor such as Kubernetes may forcefully terminate the process.

## API

### `Init`

```go
shutdown.Init(shutdown.Options{
	GracePeriod:      30 * time.Second,
	NoSignalHandling: false,
})
```

`Init` configures the package. It is safe to call more than once, but only the first call has any effect.

Options:

- `GracePeriod`: total time allotted for shutdown work. Defaults to `30s`.
- `NoSignalHandling`: disables the built-in `SIGTERM`/`SIGINT` handler. Use this if your application wants to call `shutdown.Shutdown()` itself.

### `Ctx`

```go
ctx := shutdown.Ctx(context.Background())
```

`Ctx` returns a child context that is cancelled when shutdown begins. Its cancellation cause is `shutdown.ErrShuttingDown`.

### `Handle`

```go
unregister := shutdown.Handle(func(ctx context.Context) {
	// Clean up resources here.
})
defer unregister()
```

`Handle` registers a cleanup function. Handlers receive a context with a deadline and cancellation cause `shutdown.ErrShutdownDeadline`.

The returned function unregisters the handler if shutdown has not already started.

### `Shutdown`

```go
shutdown.Shutdown()
```

`Shutdown` starts the shutdown sequence manually. It is useful when `NoSignalHandling` is enabled or when another part of your application decides the process should stop.

`Shutdown` only runs once. Concurrent or later calls have no additional effect.

## Notes

- Register handlers for cleanup work that must complete before process exit, such as closing servers, flushing queues, or draining workers.
- Use `shutdown.Ctx` for long-running loops that should stop accepting new work when shutdown begins.
- If `Handle` is called after shutdown has already started, the handler is run immediately with the active shutdown deadline.
