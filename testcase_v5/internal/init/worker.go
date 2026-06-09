package initphase

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/percona-cs/cs0055422-tc-idr/internal/blobgen"
	"github.com/percona-cs/cs0055422-tc-idr/internal/config"
	"github.com/percona-cs/cs0055422-tc-idr/internal/debugsql"
)

// teamJob describes the rows one worker is responsible for seeding.
type teamJob struct {
	idx      int   // 0-based index in the team list
	teamID   int64 // team_id value to write
	startID  int64 // first row's id (StartID + idx * RowsPerTeam)
	rowCount int
}

// teamCounters records the per-team timing/throughput numbers the summary
// table consumes.
type teamCounters struct {
	teamID   int64
	rows     int
	bytes    int64
	batches  int
	duration time.Duration
}

// seedTeam runs one team's seeding pass: generate, batch, INSERT, count-
// verify. db is the shared write pool. logger has component=init bound.
func seedTeam(ctx context.Context, db *sql.DB, cfg *config.Config, cycle blobgen.Cycle,
	job teamJob, sqlOnce *debugsql.Once, logger *slog.Logger) (teamCounters, error) {

	tc := teamCounters{teamID: job.teamID}
	start := time.Now()
	teamLogger := logger.With(
		slog.Int64("team_id", job.teamID),
		slog.Int("team_idx", job.idx),
		slog.Int("rows_target", job.rowCount),
	)
	teamLogger.Info("team start", slog.String("event", "team_start"))

	in := blobgen.Inputs{
		TeamID:   job.teamID,
		StartID:  job.startID,
		DateBase: cfg.Init.DateBase,
		DateStep: cfg.Init.DateStep,
		Seed:     cfg.Init.Seed + uint64(job.idx), // v4 mirror
	}

	buf := make([]blobgen.Row, 0, cfg.Init.InsertBatchRows)
	var bufBytes int
	batchIdx := 0

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		stmt, params := BuildInsert(
			cfg.Database.Write.Schema, cfg.Database.Write.Table, buf)
		sqlOnce.Log(logger, "init", "insert", stmt)
		if _, err := db.ExecContext(ctx, stmt, params...); err != nil {
			return fmt.Errorf("team %d batch %d insert: %w", job.teamID, batchIdx, err)
		}
		batchIdx++
		tc.batches = batchIdx
		tc.rows += len(buf)
		tc.bytes += int64(bufBytes)
		teamLogger.Info("batch flushed",
			slog.Int("batch", batchIdx),
			slog.Int("rows_in_batch", len(buf)),
			slog.Int("bytes_in_batch", bufBytes),
			slog.Int("rows_seeded", tc.rows),
		)
		buf = buf[:0]
		bufBytes = 0
		return nil
	}

	progressEvery := max(1, cfg.Init.TeamProgressRows)
	nextProgress := progressEvery

	for rowN := uint64(0); rowN < uint64(job.rowCount); rowN++ {
		if err := ctx.Err(); err != nil {
			return tc, err
		}
		in.Class = cycle.Pick(rowN)
		row := blobgen.Generate(rowN, in)
		rowBytes := RowBytes(row)
		if len(buf) > 0 &&
			(len(buf) >= cfg.Init.InsertBatchRows ||
				bufBytes+rowBytes > cfg.Init.InsertBatchBytes) {
			if err := flush(); err != nil {
				return tc, err
			}
		}
		buf = append(buf, row)
		bufBytes += rowBytes
		if tc.rows+len(buf) >= nextProgress {
			teamLogger.Info("team progress",
				slog.Int("rows_generated", tc.rows+len(buf)),
				slog.Int("rows_target", job.rowCount),
			)
			nextProgress += progressEvery
		}
	}
	if err := flush(); err != nil {
		return tc, err
	}

	// Count-verify per SPEC §4.3 Init phase.
	if err := verifyTeamCount(ctx, db, cfg, job, teamLogger); err != nil {
		return tc, err
	}
	tc.duration = time.Since(start)
	teamLogger.Info("team complete",
		slog.String("event", "team_complete"),
		slog.Int("rows", tc.rows),
		slog.Int64("bytes", tc.bytes),
		slog.Int("batches", tc.batches),
		slog.Float64("rows_per_sec", float64(tc.rows)/tc.duration.Seconds()),
		slog.Duration("elapsed", tc.duration),
	)
	return tc, nil
}

func verifyTeamCount(ctx context.Context, db *sql.DB, cfg *config.Config,
	job teamJob, logger *slog.Logger) error {

	q := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` WHERE team_id = ?",
		cfg.Database.Write.Schema, cfg.Database.Write.Table)
	var got int
	if err := db.QueryRowContext(ctx, q, job.teamID).Scan(&got); err != nil {
		return fmt.Errorf("count verify team %d: %w", job.teamID, err)
	}
	if got != job.rowCount {
		return fmt.Errorf(
			"count verify team %d: got %d want %d", job.teamID, got, job.rowCount)
	}
	logger.Info("team count verified", slog.Int("count", got))
	return nil
}
