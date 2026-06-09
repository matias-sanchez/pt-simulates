package runphase

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/config"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/db"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/debugsql"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/noise"
	"github.com/matias-sanchez/pt-simulates/testcase_v5/internal/repllag"
)

// Orchestrate runs the scan-encrypt-update phase across every team in
// cfg.Init. Reads come from cfg.Database.Read (replica),
// writes from cfg.Database.Write (master). One *sql.DB per endpoint; per-team
// workers share the pools.
func Orchestrate(ctx context.Context, cfg *config.Config, logger *slog.Logger) (Summary, error) {
	summary := Summary{Started: time.Now().UTC()}

	readPool, err := db.Open(cfg.Database.Read, cfg.Run.MaxParallelTeams)
	if err != nil {
		return summary, fmt.Errorf("open read pool: %w", err)
	}
	defer func() { _ = readPool.Close() }()

	writePool, err := db.Open(cfg.Database.Write, cfg.Run.MaxParallelTeams)
	if err != nil {
		return summary, fmt.Errorf("open write pool: %w", err)
	}
	defer func() { _ = writePool.Close() }()

	ctx, cleanup := withCancellation(ctx, cfg.Safety.MaxRuntimeSeconds)
	defer cleanup()

	// runCtx is the shared outer context. We cancel it explicitly once all
	// team workers + dispatcher return so repllag.Watch (which only exits on
	// ctx cancellation or lag breach) can shut down cleanly.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	g, gctx := errgroup.WithContext(runCtx)
	var lagCoord repllag.Coordinator
	sqlOnce := debugsql.New(cfg.Debug.LogSQLOnce)

	interval := time.Duration(cfg.Run.ReplicationLogIntervalSeconds) * time.Second
	g.Go(lagCoord.GoWatch(gctx, readPool, interval,
		int64(cfg.Init.MaxReplicationLagSeconds),
		logger.With(slog.String("component", "repllag"))))

	// Optional config-gated sidecars: intra-table noise and read-path
	// diversification searchers. Each is a no-op when its config block is
	// disabled.
	launchSidecars(gctx, g, writePool, cfg, logger)

	// Team queue.
	teams := planTeams(cfg)
	summary.TeamCount = len(teams)
	jobCh := make(chan int64, cfg.Run.MaxParallelTeams)
	resultCh := make(chan teamSummary, len(teams))

	var teamWg sync.WaitGroup
	teamWg.Add(cfg.Run.MaxParallelTeams + 1) // workers + dispatcher

	var rowsDone atomic.Int64
	for w := 0; w < cfg.Run.MaxParallelTeams; w++ {
		workerID := w + 1
		g.Go(func() error {
			defer teamWg.Done()
			for teamID := range jobCh {
				if err := gctx.Err(); err != nil {
					return err
				}
				ts, err := processTeam(gctx, readPool, writePool, cfg, teamID, workerID, sqlOnce, logger)
				if err != nil {
					return err
				}
				resultCh <- ts
				rowsDone.Add(int64(ts.rows))
			}
			return nil
		})
	}

	g.Go(func() error {
		defer teamWg.Done()
		defer close(jobCh)
		for _, t := range teams {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case jobCh <- t:
			}
		}
		return nil
	})

	// Sentinel: when all team-side goroutines finish, cancel runCtx so the
	// lag watcher exits its sleep loop and returns context.Canceled.
	//
	// Exception: when duration-bounded sidecars (noise / search_mix) are active
	// AND a max_runtime deadline is set, do NOT end the run at IDR-sweep
	// completion — let the sidecars keep driving the workload until the deadline
	// (or a signal). This is what makes a soak last its configured duration
	// rather than ending after one IDR pass.
	soakDriven := (cfg.Noise.Enabled || cfg.Run.SearchMix.Enabled) &&
		cfg.Safety.MaxRuntimeSeconds > 0
	go func() {
		teamWg.Wait()
		if !soakDriven {
			cancelRun()
		}
	}()

	waitErr := g.Wait()
	waitErr = lagCoord.Prefer(waitErr)
	if waitErr != nil {
		summary.Err = waitErr
	}
	close(resultCh)
	summary.collect(resultCh)
	summary.Ended = time.Now().UTC()
	summary.Duration = summary.Ended.Sub(summary.Started)

	if summary.Err != nil {
		return summary, summary.Err
	}
	logger.Info("run complete",
		slog.String("event", "run_complete"),
		slog.Int("rows", summary.Rows),
		slog.Int("batches", summary.Batches),
		slog.Duration("elapsed", summary.Duration),
	)
	return summary, nil
}

// launchSidecars starts the optional, config-gated sidecar workers: intra-table
// noise DML and read-path diversification
// searchers. Each is a no-op when its config
// block is disabled. Extracted from Orchestrate to keep it within the funlen
// budget.
func launchSidecars(ctx context.Context, g *errgroup.Group, writePool *sql.DB,
	cfg *config.Config, logger *slog.Logger) {
	g.Go(func() error {
		return noise.Run(ctx, writePool, cfg, logger.With(slog.String("component", "noise")))
	})
	g.Go(func() error {
		return runSearchMix(ctx, cfg, logger.With(slog.String("component", "search_mix")))
	})
}

func planTeams(cfg *config.Config) []int64 {
	out := make([]int64, cfg.Init.Teams)
	for i := 0; i < cfg.Init.Teams; i++ {
		out[i] = cfg.Init.TeamIDBase + int64(i)
	}
	return out
}

// collect folds the per-team results channel into the run summary totals.
func (s *Summary) collect(resultCh <-chan teamSummary) {
	for ts := range resultCh {
		s.Teams = append(s.Teams, ts)
		s.Rows += ts.rows
		s.Batches += ts.batches
		s.Selects += ts.selects
		s.Updates += ts.updates
	}
}

// Summary captures per-run totals + per-team breakdown for the run-phase
// artifact.
type Summary struct {
	Started   time.Time
	Ended     time.Time
	Duration  time.Duration
	TeamCount int
	Rows      int
	Batches   int
	Selects   int
	Updates   int
	Teams     []teamSummary
	Err       error
}
