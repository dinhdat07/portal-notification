package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"portal-notification/internal/observability"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

func (a *App) Run() error {
	slog.Info("notification service runtime starting")

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	defer func() {
		if a.KafkaReader == nil {
			return
		}

		if err := a.KafkaReader.Close(); err != nil {
			slog.Error("failed to close kafka reader", slog.Any("error", err))
			return
		}

		slog.Info("kafka reader closed")
	}()

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return a.EmailWorker.Run(gCtx)
	})

	g.Go(func() error {
		return a.RetryWorker.Run(gCtx)
	})

	adminAddr := ":" + a.MetricsPort
	srv := &http.Server{
		Addr:    adminAddr,
		Handler: observability.NewMux(a.readinessReport),
	}

	g.Go(func() error {
		slog.Info("admin HTTP server listening", slog.String("addr", adminAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("admin HTTP server failed: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		<-gCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		slog.Info("stopping admin HTTP server")
		return srv.Shutdown(shutdownCtx)
	})

	err := g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	slog.Info("notification service stopped cleanly")
	return nil
}
