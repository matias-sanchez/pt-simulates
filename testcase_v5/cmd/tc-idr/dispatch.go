package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/percona-cs/cs0055422-tc-idr/internal/artifacts"
	"github.com/percona-cs/cs0055422-tc-idr/internal/config"
)

func runInit(ctx context.Context, cfg *config.Config, commit, builtAt string) error {
	return initEntrypoint(ctx, cfg, commit, builtAt)
}

func runWorkload(ctx context.Context, cfg *config.Config, commit, builtAt string) error {
	return runEntrypoint(ctx, cfg, commit, builtAt)
}

func printLatestStatus(cfg *config.Config) error {
	return artifacts.PrintStatus(cfg.Artifacts.Dir, os.Stdout)
}

func tailLatestLog(ctx context.Context, cfg *config.Config) error {
	dir, err := artifacts.LatestDir(cfg.Artifacts.Dir)
	if err != nil {
		return err
	}
	path, err := dir.LogFile()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "tailing %s (run_id=%s)\n", path, dir.RunID)
	err = artifacts.FollowLog(ctx, path, os.Stdout)
	if err != nil && errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func signalLatest(cfg *config.Config) error {
	dir, err := artifacts.LatestDir(cfg.Artifacts.Dir)
	if err != nil {
		return err
	}
	pid, err := artifacts.ReadPID(dir)
	if err != nil {
		return err
	}
	if err := artifacts.SignalPID(pid, syscall.SIGTERM); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "sent SIGTERM to pid %d (run_id=%s)\n", pid, dir.RunID)
	return nil
}
