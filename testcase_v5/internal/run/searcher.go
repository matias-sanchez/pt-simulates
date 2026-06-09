package runphase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/percona-cs/cs0055422-tc-idr/internal/config"
	"github.com/percona-cs/cs0055422-tc-idr/internal/db"
)

// runSearchMix drives the optional read-path diversification searchers (SPEC §2
// Exception 2026-05-28, Track A1). With cfg.Run.SearchMix.Enabled=false this is
// a no-op and the harness behaves exactly as before. When enabled it opens its
// OWN read pool against the replica (so it never perturbs the IDR loop's plans
// or pool) and runs read-only SELECT shapes flat-out to maximise the rate and
// diversity of the btr_cur_search_to_nth_level -> buf_page_get_gen ->
// single_page -> rw_lock_s_lock latch path across all three observed crash
// callers. Searchers never write. A genuine searcher error aborts the run
// (CONSTITUTION P3); context cancellation at shutdown is not an error.
func runSearchMix(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	s := cfg.Run.SearchMix
	if !s.Enabled {
		logger.Info("search_mix disabled; skipping")
		return nil
	}
	workers := s.RangeEstimateWorkers + s.MRRWorkers + s.ForwardRefScanWorkers
	readPool, err := db.Open(cfg.Database.Read, workers)
	if err != nil {
		return fmt.Errorf("search_mix open read pool: %w", err)
	}
	defer func() { _ = readPool.Close() }()

	rows := s.RowsPerQuery
	if rows < 1 {
		rows = 10
	}
	logEvery := time.Duration(s.LogIntervalSeconds) * time.Second
	if logEvery <= 0 {
		logEvery = 60 * time.Second
	}
	schema, table := cfg.Database.Read.Schema, cfg.Database.Read.Table
	teamLo := cfg.Init.TeamIDBase
	teamHi := cfg.Init.TeamIDBase + int64(cfg.Init.Teams) - 1
	if teamHi < teamLo {
		teamHi = teamLo
	}
	rowsPerTeam := int64(cfg.Init.RowsPerTeam)
	if rowsPerTeam < 1 {
		rowsPerTeam = 1
	}
	dateLo := cfg.Init.DateBase
	dateSpan := rowsPerTeam * cfg.Init.DateStep
	if dateSpan < 1 {
		dateSpan = 1
	}

	logger.Info("search_mix start",
		slog.String("event", "start"),
		slog.Int("range_estimate_workers", s.RangeEstimateWorkers),
		slog.Int("mrr_workers", s.MRRWorkers),
		slog.Int("forward_refscan_workers", s.ForwardRefScanWorkers),
		slog.Int("rows_per_query", rows),
	)

	g, gctx := errgroup.WithContext(ctx)
	launch := func(kind, query string, mrr bool, count int) {
		for i := 0; i < count; i++ {
			workerID := i + 1
			g.Go(func() error {
				return searchWorker(gctx, readPool, searchSpec{
					kind: kind, query: query, mrr: mrr, rows: rows,
					teamLo: teamLo, teamHi: teamHi,
					startID: cfg.Init.StartID, rowsPerTeam: rowsPerTeam,
					dateLo: dateLo, dateSpan: dateSpan,
					logEvery: logEvery, workerID: workerID,
				}, logger)
			})
		}
	}
	// Read-path fidelity gate, captured automatically (not by hand): log the
	// dominant forward_refscan EXPLAIN once at startup so the chosen access path
	// (team_id secondary ref + filesort -> clustered-root descent) lands in the
	// run artifacts and any plan drift is visible. Like the IDR loop's EXPLAIN it
	// LOGS the optimizer's choice rather than asserting it, to avoid masking a
	// diagnostic signal. forwardRefScanSQL binds (team_id, id_cursor); id>0 plans
	// the same access path the workers drive.
	if s.ForwardRefScanWorkers > 0 {
		// Skip context cancellation (a shutdown racing startup) the same way the
		// searcher workers and the inner g.Wait() do below — a clean stop is not an
		// explain failure.
		if err := logExplainOnce(gctx, readPool, forwardRefScanSQL(schema, table, rows),
			teamLo, logger.With(slog.String("shape", "forward_refscan"))); err != nil &&
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("search_mix forward_refscan explain: %w", err)
		}
	}

	launch("range_estimate", rangeEstimateSQL(schema, table, rows), false, s.RangeEstimateWorkers)
	launch("mrr", mrrSQL(schema, table, rows), true, s.MRRWorkers)
	launch("forward_refscan", forwardRefScanSQL(schema, table, rows), false, s.ForwardRefScanWorkers)

	// A clean shutdown cancels gctx; only a non-cancellation error is a real
	// failure (mirrors internal/repllag coordinator discrimination).
	if err := g.Wait(); err != nil &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	logger.Info("search_mix complete", slog.String("event", "complete"))
	return nil
}

// searchSpec carries one searcher worker's immutable parameters.
type searchSpec struct {
	kind, query          string
	mrr                  bool
	rows                 int
	teamLo, teamHi       int64
	startID, rowsPerTeam int64
	dateLo, dateSpan     int64
	logEvery             time.Duration
	workerID             int
}

// searchWorker pins one connection (so SET SESSION sticks for the MRR shape)
// and issues the shape flat-out, rotating team/range bounds so each execution
// is a distinct, non-cached plan. It drains every result row to force the
// clustered fetch / index descent, then discards the data.
func searchWorker(ctx context.Context, pool *sql.DB, spec searchSpec, logger *slog.Logger) error {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("search_mix %s conn: %w", spec.kind, err)
	}
	defer func() { _ = conn.Close() }()
	// State fidelity: pin REPEATABLE-READ on the searcher session so the read
	// view matches the crash-time fingerprint (all 5 cores: isolation=
	// REPEATABLE_READ) regardless of the replica's server default.
	if _, err := conn.ExecContext(ctx,
		"SET SESSION transaction_isolation = 'REPEATABLE-READ'"); err != nil {
		return fmt.Errorf("search_mix %s set isolation: %w", spec.kind, err)
	}
	if spec.mrr {
		if _, err := conn.ExecContext(ctx,
			"SET SESSION optimizer_switch = 'mrr=on,mrr_cost_based=off'"); err != nil {
			return fmt.Errorf("search_mix %s set optimizer_switch: %w", spec.kind, err)
		}
	}

	wl := logger.With(slog.String("shape", spec.kind), slog.Int("worker_id", spec.workerID))
	var queries atomic.Int64
	var n int64
	lastLog := time.Now()
	span := spec.teamHi - spec.teamLo + 1
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		team := spec.teamLo + (n % span)
		args := spec.argsFor(team, n)
		// A genuine query error aborts (P3); runSearchMix filters the
		// context-cancellation case that races shutdown.
		if err := runOneSearch(ctx, conn, spec.query, args); err != nil {
			return fmt.Errorf("search_mix %s query: %w", spec.kind, err)
		}
		queries.Add(1)
		n++
		if time.Since(lastLog) >= spec.logEvery {
			wl.Info("search progress", slog.Int64("queries", queries.Load()))
			lastLog = time.Now()
		}
	}
}

// argsFor builds the bind arguments for one execution, advancing the bounds by
// the iteration counter so each execution re-runs the scan (MySQL 8.0 has no
// result cache; the prepared plan is reused with fresh binds). Bind values assume
// the blobgen seed layout (id = StartID + teamIdx*RowsPerTeam + rowN, external_id
// = "ext-<id>", is_deleted = is_public = 0). forward_refscan is robust to a
// non-contiguous seed (its cursor stays at/below the team's first id, so
// id>cursor still returns rows); mrr/range_estimate rely on that layout to
// materialise rows and drive their clustered/index descent.
func (spec searchSpec) argsFor(team, n int64) []any {
	lo := spec.dateLo + (n*7)%spec.dateSpan
	hi := lo + spec.dateSpan/4 + 1
	teamIdx := team - spec.teamLo
	teamStartID := spec.startID + teamIdx*spec.rowsPerTeam
	switch spec.kind {
	case "range_estimate":
		// is_deleted/is_public are 0 for every seeded row; bind 0 so the range
		// is non-empty while the date window varies per execution.
		return []any{team, 0, 0, lo, hi}
	case "mrr":
		// external_id values that actually exist for this team, so the IN-list
		// resolves real keys and MRR performs the clustered fetch.
		args := []any{team, 0}
		for i := 0; i < spec.rows; i++ {
			r := (n + int64(i)) % spec.rowsPerTeam
			args = append(args, fmt.Sprintf("ext-%d", teamStartID+r))
		}
		return args
	default: // forward_refscan: team_id, id keyset cursor over the team's seeded
		// id range (id = teamStartID + rowN). Bound the cursor to the lower
		// (rowsPerTeam - rows) of the range so at least `rows` ids always satisfy
		// id>cursor — guaranteeing the LIMIT-N clustered fetch (the clustered-root
		// descent we recreate) fires on EVERY iteration, not only when the cursor
		// happens to land low. The cursor still advances per iteration (distinct,
		// non-cached plans).
		idMod := spec.rowsPerTeam - int64(spec.rows)
		if idMod < 1 {
			idMod = 1
		}
		return []any{team, teamStartID + (n % idMod)}
	}
}

// runOneSearch executes the query and drains all rows generically (column
// count is not known at compile time across shapes) to force row materialisation.
func runOneSearch(ctx context.Context, conn *sql.Conn, query string, args []any) error {
	rs, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rs.Close() }()
	cols, err := rs.Columns()
	if err != nil {
		return err
	}
	sink := make([]any, len(cols))
	for i := range sink {
		sink[i] = new(sql.RawBytes)
	}
	for rs.Next() {
		if err := rs.Scan(sink...); err != nil {
			return err
		}
	}
	return rs.Err()
}
