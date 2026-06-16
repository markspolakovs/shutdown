// Package shutdown implements a framework for gracefully handling application shutdowns.
package shutdown

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type key struct{}

var (
	_stoppers = make(map[*key]context.CancelCauseFunc)
	_handlers = make(map[*key]HandlerFunc)
	_shutdownStarted = make(chan struct{})
	_shutdownDeadline time.Time
	_lateShutdownWG sync.WaitGroup
	_mux sync.Mutex
)

const defaultGracePeriod = 30 * time.Second

var (
	_gracePeriod = defaultGracePeriod // default; overridden by Init
	_noExit bool
)

var (
	_initer sync.Once
	_shutdowner sync.Once
)

var (
	// ErrShuttingDown is the error that contexts returned by Ctx will be cancelled with.
	ErrShuttingDown = errors.New("shutting down")
	// ErrShutdownDeadline is the error that contexts given to HandlerFuncs will be cancelled with.
	ErrShutdownDeadline = errors.New("shutdown deadline exceeded")
)

type Options struct {
	// GracePeriod is the time allotted for all shutdown handlers to finish their work.
	// For example, in Kubernetes the default is 30 seconds.
	// GracePeriod defaults to 30 seconds if not set.
	GracePeriod      time.Duration
	// If set, the global SIGTERM/SIGINT handler will not be configured and the
	// application will be responsible for calling Shutdown itself.
	NoSignalHandling bool
	// If set, a shutdown will not cause the application to exit.
	NoExit bool
}

// Init sets up the shutdown handling. It should be called once at the start of the program.
// Repeated calls have no effect.
func Init(opts Options) {
	_initer.Do(func() {
		if opts.GracePeriod > 0 {
			_gracePeriod = opts.GracePeriod.Abs()
		}
		_noExit = opts.NoExit

		if !opts.NoSignalHandling {
			ch := make(chan os.Signal, 1)
			go func() {
				<-ch
				signal.Stop(ch)
				Shutdown()
			}()
			signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
		}
	})
}

// Ctx returns a context based on the given ctx that will be cancelled
// when a shutdown is requested. If shutdown has already begun, the returned
// context is immediately cancelled with ErrShuttingDown.
//
// Ctx is safe for concurrent use. Each call to Ctx returns a new context.
func Ctx(ctx context.Context) context.Context {

	child, cancel := context.WithCancelCause(ctx)
	key := new(key)

	_mux.Lock()
	_stoppers[key] = cancel
	shuttingDown := !_shutdownDeadline.IsZero()
	_mux.Unlock()

	context.AfterFunc(child, func() {
		_mux.Lock()
		defer _mux.Unlock()
		delete(_stoppers, key)
	})

	if shuttingDown {
		cancel(ErrShuttingDown)
	}

	return child
}

// HandlerFunc is the type of shutdown handler functions.
// When a shutdown is requested, it will be called with a context that represents
// the deadline for the application to complete any cleanup work. This deadline
// will be no greater than the grace period, but may be smaller to allow for tolerance.
type HandlerFunc func(context.Context)

// Handle registers a shutdown handler function.
//
// The handler will be called in a separate goroutine when a shutdown is
// requested, with a context whose deadline is no later than the configured
// grace period.
//
// If shutdown has already begun, the handler will be called immediately in a
// separate goroutine with the shutdown deadline context.
//
// Each handler is invoked at most once. Handlers that are still registered
// when shutdown begins are guaranteed to be invoked.
//
// Handle is safe for concurrent use, including concurrently with Shutdown.
//
// Handle returns a deregister function used to exclude this handler from future
// shutdowns. Calling it once a shutdown has begun has no effect.
func Handle(handler HandlerFunc) func() {
	_mux.Lock()
	deadline := _shutdownDeadline
	if !deadline.IsZero() {
		// a shutdown is in progress, call it immediately
		_mux.Unlock()
		_lateShutdownWG.Go(func() {
			ctx, done := context.WithDeadlineCause(context.Background(), deadline, ErrShutdownDeadline)
			defer done()
			handler(ctx)
		})
		return func(){}
	}

	key := new(key)
	_handlers[key] = handler
	_mux.Unlock()
	return func() {
		_mux.Lock()
		defer _mux.Unlock()
		delete(_handlers, key)
	}
}

// Wait blocks until a shutdown has been initiated and all shutdown handlers
// have completed. It is intended for use in main() to ensure that the main
// goroutine (and thus the entire program) does not exist until shutdown
// is complete.
//
// If Shutdown has not yet been called, Wait blocks until it is called and
// all handlers have finished. If Shutdown has already completed, Wait
// returns immediately. Wait is typically used with NoExit: true.
// Without it, Wait will never return as os.Exit will be called first.
func Wait() {
    <-_shutdownStarted
    _lateShutdownWG.Wait()
}

// Shutdown starts a shutdown of the application.
//
// Shutdown first cancels all contexts returned by Ctx, then invokes
// all registered shutdown handlers. Once every handler has finished,
// shutdown exits the application unless Init was called with NoExit set to true.
//
// Shutdown only runs once. Concurrent or later calls have no effect and will
// block until the first Shutdown call returns.
func Shutdown() {
	_shutdowner.Do(func() {
		_mux.Lock()
		// Use 80% of the grace period so the process has a buffer to clean up
		// after handlers finish before the external deadline (e.g. Kubernetes SIGKILL) arrives.
		timeout := time.Duration(float64(_gracePeriod) * 0.8)
		deadline := time.Now().Add(timeout)
		_shutdownDeadline = deadline

		// Clone the maps to avoid holding _mux while calling handlers
		stoppers := make([]context.CancelCauseFunc, 0, len(_stoppers))
		for _, stop := range _stoppers {
		    stoppers = append(stoppers, stop)
		}
		handlers := make([]HandlerFunc, 0, len(_handlers))
		for _, h := range _handlers {
		    handlers = append(handlers, h)
		}
		_lateShutdownWG.Add(1) // sentinel: prevents _lateShutdownWG.Wait() returning before all late handlers are launched
		close(_shutdownStarted) // only after the above to ensure that _lateShutdownWG is >=1 whenever any Wait() reaches _lateShutdownWG.Wait()
		_mux.Unlock()

		for _, cancel := range stoppers {
			cancel(ErrShuttingDown)
		}

		ctx, done := context.WithDeadlineCause(context.Background(), deadline, ErrShutdownDeadline)
		defer done() // NB: no-op outside of tests because of the os.Exit()

		var wg sync.WaitGroup
		for _, fn := range handlers {
			wg.Go(func() {
				fn(ctx)
			})
		}
		wg.Wait()
		_lateShutdownWG.Done()
		_lateShutdownWG.Wait()
		if !_noExit {
			os.Exit(0)
		}
	})
}
