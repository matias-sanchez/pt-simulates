package runphase

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/percona-cs/cs0055422-tc-idr/internal/config"
	"github.com/percona-cs/cs0055422-tc-idr/internal/debugsql"
)

// teamRowResult is the minimal scan-target we pull out of the SELECT — we
// only need the three plaintext blobs + id + team_id + cursor field. The
// remaining 37 columns are pulled to match v4 wire shape but discarded.
type teamRowResult struct {
	id                int64
	teamID            int64
	dateCreate        int64
	contents          []byte
	contentsHighlight []byte
	metadata          string
}

// scanTargets returns 43 pointers in the same order as blobgen.Columns().
// Throwaway columns land in sinks; the six we keep land in r.
func (r *teamRowResult) scanTargets(sink *rowSinks) []any {
	return []any{
		&r.id,                       // id
		&r.teamID,                   // team_id
		&sink.userID,                // user_id
		&r.dateCreate,               // date_create
		&sink.secret, &sink.pubSec,  // secret, pub_secret
		&sink.size, &sink.isStored,  // size, is_stored
		&sink.origName, &sink.storedName, &sink.title, &sink.mimetype,
		&sink.parentID,
		&r.contents, &sink.contentsEKM,
		&r.contentsHighlight, &sink.contentsHighlightEKM,
		&sink.highlightType,
		&r.metadata, &sink.metadataEKM,
		&sink.externalURL,
		&sink.isDeleted, &sink.dateDelete,
		&sink.isPublic, &sink.pubShared,
		&sink.lastIndexed,
		&sink.source, &sink.externalID,
		&sink.serviceTypeID, &sink.serviceID,
		&sink.isMultiteam, &sink.originalTeamID, &sink.teamsSharedWith,
		&sink.serviceTeamID,
		&sink.isTombstoned, &sink.thumbnailVersion, &sink.dateThumbnailRetrieved,
		&sink.externalPtr,
		&sink.md5,
		&sink.metadataVersion, &sink.unencryptedMetadata,
		&sink.restrictionType, &sink.version,
	}
}

// rowSinks holds throwaway destinations for the 37 columns we read on the
// wire but do not use in the update. SQL NULLs need NullX types.
type rowSinks struct {
	userID                                                  sql.NullInt64
	secret, pubSec                                          sql.NullString
	size                                                    sql.NullInt64
	isStored                                                sql.NullInt64
	origName, storedName, title, mimetype                   sql.NullString
	parentID                                                sql.NullInt64
	contentsEKM, contentsHighlightEKM                       []byte
	highlightType                                           sql.NullString
	metadataEKM                                             []byte
	externalURL                                             sql.NullString
	isDeleted                                               sql.NullInt64
	dateDelete                                              sql.NullInt64
	isPublic, pubShared                                     sql.NullInt64
	lastIndexed                                             sql.NullInt64
	source, externalID                                      sql.NullString
	serviceTypeID, serviceID                                sql.NullInt64
	isMultiteam, originalTeamID                             sql.NullInt64
	teamsSharedWith                                         sql.NullString
	serviceTeamID                                           sql.NullInt64
	isTombstoned, thumbnailVersion, dateThumbnailRetrieved  sql.NullInt64
	externalPtr                                             []byte
	md5                                                     sql.NullString
	metadataVersion                                         sql.NullInt64
	unencryptedMetadata                                     sql.NullString
	restrictionType, version                                sql.NullInt64
}

// teamSummary captures one team's run-phase totals for the artifact table.
type teamSummary struct {
	teamID   int64
	workerID int
	rows     int
	batches  int
	selects  int
	updates  int
	elapsed  time.Duration
}

// processTeam drives the SPEC §5.4 inner loop for one team. read and write
// are the shared pools; ctx cancellation aborts the loop between batches.
// First batch logs EXPLAIN once and the per-batch timings once (then the
// regular team_log_interval cadence takes over).
func processTeam(ctx context.Context, read, write *sql.DB, cfg *config.Config,
	teamID int64, workerID int, sqlOnce *debugsql.Once, logger *slog.Logger) (teamSummary, error) {

	teamLogger := logger.With(
		slog.Int64("team_id", teamID),
		slog.Int("worker_id", workerID),
	)
	sum := teamSummary{teamID: teamID, workerID: workerID}
	start := time.Now()
	teamLogger.Info("team start", slog.String("event", "team_start"))

	sel := selectSQL(cfg.Database.Read.Schema, cfg.Database.Read.Table, cfg.Run.BatchSize)
	upd := updateSQL(cfg.Database.Write.Schema, cfg.Database.Write.Table)

	loggedExplain := false
	var cursor int64
	logEvery := max(1, cfg.Run.TeamLogIntervalBatches)
	loggedFirstBatch := false

	for {
		if err := ctx.Err(); err != nil {
			return sum, err
		}
		if !loggedExplain {
			sqlOnce.Log(teamLogger, "run", "select", sel)
			if err := logExplainOnce(ctx, read, sel, teamID, teamLogger); err != nil {
				return sum, fmt.Errorf("explain team %d: %w", teamID, err)
			}
			loggedExplain = true
		}
		selStart := time.Now()
		rows, err := fetchBatch(ctx, read, sel, teamID, cursor)
		if err != nil {
			return sum, fmt.Errorf("select team %d: %w", teamID, err)
		}
		selMs := time.Since(selStart).Milliseconds()
		sum.selects++
		if len(rows) == 0 {
			break
		}
		cursor = rows[len(rows)-1].dateCreate

		encStart := time.Now()
		updates := buildEKMUpdates(rows)
		encMs := time.Since(encStart).Milliseconds()

		updStart := time.Now()
		if err := applyUpdates(ctx, write, upd, updates, sqlOnce, teamLogger); err != nil {
			return sum, err
		}
		updMs := time.Since(updStart).Milliseconds()

		sum.rows += len(rows)
		sum.batches++
		sum.updates += len(rows)

		if !loggedFirstBatch || sum.batches%logEvery == 0 {
			teamLogger.Info("batch",
				slog.Int("batch", sum.batches),
				slog.Int("rows_in_batch", len(rows)),
				slog.Int64("cursor", cursor),
				slog.Int64("select_ms", selMs),
				slog.Int64("encrypt_ms", encMs),
				slog.Int64("update_ms", updMs),
			)
			loggedFirstBatch = true
		}
	}
	sum.elapsed = time.Since(start)
	teamLogger.Info("team complete",
		slog.String("event", "team_complete"),
		slog.Int("rows", sum.rows),
		slog.Int("batches", sum.batches),
		slog.Duration("elapsed", sum.elapsed),
	)
	return sum, nil
}

func fetchBatch(ctx context.Context, db *sql.DB, sel string,
	teamID, cursor int64) ([]teamRowResult, error) {

	rs, err := db.QueryContext(ctx, sel, teamID, cursor)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	var out []teamRowResult
	sink := &rowSinks{}
	for rs.Next() {
		var r teamRowResult
		if err := rs.Scan(r.scanTargets(sink)...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type updatePlan struct {
	id, teamID                            int64
	contentsEKM, highlightEKM, metadataEKM []byte
}

func buildEKMUpdates(rows []teamRowResult) []updatePlan {
	out := make([]updatePlan, len(rows))
	for i, r := range rows {
		out[i] = updatePlan{
			id:           r.id,
			teamID:       r.teamID,
			contentsEKM:  encryptBlob(r.contents, contentsEKMLabel(r.id)),
			highlightEKM: encryptBlob(r.contentsHighlight, highlightEKMLabel(r.id)),
			metadataEKM:  encryptText(r.metadata, metadataEKMLabel(r.id)),
		}
	}
	return out
}

func applyUpdates(ctx context.Context, db *sql.DB, stmt string,
	plans []updatePlan, sqlOnce *debugsql.Once, logger *slog.Logger) error {

	for _, p := range plans {
		sqlOnce.Log(logger, "run", "update", stmt)
		if _, err := db.ExecContext(ctx, stmt,
			p.contentsEKM, p.highlightEKM, p.metadataEKM,
			p.id, p.teamID); err != nil {
			return fmt.Errorf("update id=%d team=%d: %w", p.id, p.teamID, err)
		}
	}
	return nil
}
