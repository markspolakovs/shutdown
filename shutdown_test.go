package shutdown

import (
	"context"
	"sync"
	"testing"
	"time"
)

func resetForTest(t *testing.T) {
	t.Helper()
	_stoppers.Range(func(key, _ any) bool {
		_stoppers.Delete(key)
		return true
	})
	_handlers.Range(func(key, _ any) bool {
		_handlers.Delete(key)
		return true
	})
	_gracePeriod = 0
	_initer = sync.Once{}
	_shutdowner = sync.Once{}
	_shutdownDeadlineUnixMicros.Store(0)
	_shutdownWG = sync.WaitGroup{}
	_noExit = true
}

func TestInitSetsGracePeriodOnce(t *testing.T) {
	resetForTest(t)

	Init(Options{GracePeriod: 150 * time.Millisecond, NoSignalHandling: true})
	if _gracePeriod != 150*time.Millisecond {
		t.Fatalf("grace period = %v, want 150ms", _gracePeriod)
	}

	Init(Options{GracePeriod: time.Second, NoSignalHandling: true})
	if _gracePeriod != 150*time.Millisecond {
		t.Fatalf("second Init changed grace period to %v", _gracePeriod)
	}
}

func TestInitUsesDefaultGracePeriod(t *testing.T) {
	resetForTest(t)

	Init(Options{NoSignalHandling: true})
	if _gracePeriod != 30*time.Second {
		t.Fatalf("grace period = %v, want 30s", _gracePeriod)
	}
}

func TestCtxIsCancelledOnShutdown(t *testing.T) {
	resetForTest(t)
	Init(Options{GracePeriod: time.Second, NoSignalHandling: true})

	ctx := Ctx(t.Context())
	select {
	case <-ctx.Done():
		t.Fatal("context was cancelled before shutdown")
	default:
	}

	Shutdown()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context was not cancelled")
	}

	if got := context.Cause(ctx); got == nil || got.Error() != "shutting down" {
		t.Fatalf("context cause = %v, want shutting down", got)
	}
}

func TestHandleInvokesRegisteredHandlers(t *testing.T) {
	resetForTest(t)
	Init(Options{GracePeriod: time.Second, NoSignalHandling: true})

	called := make(chan struct{})
	Handle(func(ctx context.Context) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("handler context has no deadline")
		}
		close(called)
	})

	Shutdown()

	select {
	case <-called:
	default:
		t.Fatal("handler was not called")
	}
}

func TestHandleUnregistersHandlers(t *testing.T) {
	resetForTest(t)
	Init(Options{GracePeriod: time.Second, NoSignalHandling: true})

	called := false
	unregister := Handle(func(context.Context) {
		called = true
	})
	unregister()

	Shutdown()

	if called {
		t.Fatal("unregistered handler was called")
	}
}

func TestShutdownWaitsForHandlers(t *testing.T) {
	resetForTest(t)
	Init(Options{GracePeriod: time.Second, NoSignalHandling: true})

	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	Handle(func(context.Context) {
		close(started)
		<-release
	})

	go func() {
		Shutdown()
		close(returned)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	select {
	case <-returned:
		t.Fatal("Shutdown returned before handler completed")
	default:
	}

	close(release)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not return after handler completed")
	}
}
