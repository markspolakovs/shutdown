package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/markspolakovs/shutdown"
)

func main() {
	shutdown.Init(shutdown.Options{
		GracePeriod: 30 * time.Second,
	})

	server := &http.Server{Addr: ":"}

	shutdown.Handle(func(ctx context.Context) {
		fmt.Println("shutdown requested")
		_ = server.Shutdown(ctx)
		fmt.Println("server shut down")
	})

	fmt.Println("starting server")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}

	shutdown.Wait()
}
