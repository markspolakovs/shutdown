// Package shutdown implements a framework for gracefully handling application shutdowns.
package shutdown

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	_stoppers sync.Map
	_handlers sync.Map
)

var (
	_gracePeriod time.Duration
	_noExit bool
)

var (
	_initer sync.Once
	_shutdowner sync.Once
	_shutdownDeadlineUnixMicros atomic.Int64
	_shutdownWG sync.WaitGroup
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
func Init(opts Options) {
	_initer.Do(func() {
		if opts.GracePeriod > 0 {
			_gracePeriod = opts.GracePeriod.Abs()
		} else {
			_gracePeriod = 30 * time.Second
		}
		if opts.NoExit {
			_noExit = opts.NoExit
		}

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
// when a shutdown is requested.
func Ctx(ctx context.Context) context.Context {
	child, cancel := context.WithCancelCause(ctx)
	key := new(byte)
	_stoppers.Store(key, cancel)
	go func(){
		<-child.Done()
		_stoppers.Delete(key)
	}()
	if _shutdownDeadlineUnixMicros.Load() > 0 {
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
// The function will be called when a shutdown is requested,
// with a context with a deadline of no more than the grace period.
// Note that if Handle is called concurrently with Shutdown
// (or receiving a shutdown signal), there's no guarantee that
// handler will be called.
func Handle(handler HandlerFunc) func() {
	if deadline := _shutdownDeadlineUnixMicros.Load(); deadline > 0 {
		// a shutdown is in progress, call it immediately
		_shutdownWG.Go(func() {
			ctx, done := context.WithDeadlineCause(context.Background(), time.UnixMicro(deadline), ErrShutdownDeadline)
			defer done()
			handler(ctx)
		})
		return func(){}
	}

	key := new(byte)
	_handlers.Store(key, handler)
	return func() {
		_handlers.Delete(key)
	}
}

// Shutdown starts a shutdown of the application, cancelling all contexts
// returned by Ctx and invoking all handlers given to Handle. Once every
// handler has finished, shutdown exits the application unless Init
// was called with NoExit set to true.
//
// Shutdown only runs once. Concurrent or later calls have no effect and will
// block until the first Shutdown call returns.
func Shutdown() {
	_shutdowner.Do(func() {
		_shutdownWG.Add(1) // sentinel: keep counter > 0 until handlers are all launched
		timeout := time.Duration(float64(_gracePeriod) * 0.8)
		if timeout == 0 {
			timeout = 25 * time.Second
		}
		deadline := time.Now().Add(timeout)
		_shutdownDeadlineUnixMicros.Store(deadline.UnixMicro())
		ctx, done := context.WithDeadlineCause(context.Background(), deadline, ErrShutdownDeadline)
		defer done() // NB: no-op outside of tests because of the os.Exit()
		_stoppers.Range(func(_, fn any) bool {
			fn.(context.CancelCauseFunc)(ErrShuttingDown)
			return true
		})
		_handlers.Range(func(_, handler any) bool {
			_shutdownWG.Go(func() {
				handler.(HandlerFunc)(ctx)
			})
			return true
		})
		_shutdownWG.Done()
		_shutdownWG.Wait()
		if !_noExit {
			os.Exit(0)
		}
	})
}
