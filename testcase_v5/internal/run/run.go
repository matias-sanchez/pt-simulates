package runphase

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/percona-cs/cs0055422-tc-idr/internal/artifacts"
	"github.com/percona-cs/cs0055422-tc-idr/internal/config"
	"github.com/percona-cs/cs0055422-tc-idr/internal/obs"
)

// Run is the CLI's entrypoint for the run verb.
func Run(ctx context.Context, cfg *config.Config, commit, builtAt string) error {
	header := artifacts.Header(commit, builtAt, "")
	dir, err := artifacts.Open(cfg.Artifacts.Dir, header)
	if err != nil {
		return err
	}

	logPath := filepath.Join(dir.Path, "run.jsonl")
	logger, closer, err := obs.NewLoggers(logPath)
	if err != nil {
		return err
	}
	defer func() { _ = closer.Close() }()
	logger = logger.With(
		slog.String("component", "run"),
		slog.String("phase", "run"),
		slog.String("run_id", dir.RunID),
		slog.String("commit", commit),
		slog.String("built_at", builtAt),
	)

	summary, runErr := Orchestrate(ctx, cfg, logger)
	teams := make([]artifacts.TeamRow, len(summary.Teams))
	for i, t := range summary.Teams {
		teams[i] = artifacts.TeamRow{
			TeamID: t.teamID, WorkerID: t.workerID,
			Rows: t.rows, Batches: t.batches,
			Selects: t.selects, Updates: t.updates,
			Duration: t.elapsed,
		}
	}
	phase := artifacts.PhaseSummary{
		Phase: "run", Started: summary.Started, Ended: summary.Ended,
		Duration: summary.Duration, TeamCount: summary.TeamCount,
		Rows: summary.Rows, Batches: summary.Batches,
		Selects: summary.Selects, Updates: summary.Updates,
		Teams: teams,
	}
	if werr := dir.WriteSummary(phase); werr != nil {
		logger.Error("write summary failed", slog.String("err", werr.Error()))
	}
	if werr := dir.WriteHeader(header.WithEndedAt(time.Now())); werr != nil {
		logger.Error("write header end failed", slog.String("err", werr.Error()))
	}
	if runErr != nil {
		logger.Error("run failed",
			slog.String("event", "abort"),
			slog.String("err", runErr.Error()),
			slog.Duration("elapsed", summary.Duration),
		)
		return runErr
	}
	return nil
}
