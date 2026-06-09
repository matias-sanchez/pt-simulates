package noise

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

type updateWorker struct {
	id             int
	write          *sql.DB
	schema         string
	table          string
	ratePerSec     int
	schedule       BurstSchedule
	teamLo         int64
	teamHi         int64
	startID        int64
	rowsPerTeam    int64
	hotRowsPerTeam int64 // 0 = full team range
	counters       *Counters
	logger         *slog.Logger
}

func (w *updateWorker) run(ctx context.Context) error {
	stmt := buildUpdateStmt(w.schema, w.table)
	start := time.Now()
	walker := &linWalker{state: uint64(w.id)*0xBF58476D1CE4E5B9 + uint64(w.startID)}

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		elapsed := time.Since(start)
		if d := rateSleep(w.ratePerSec, w.schedule, elapsed); d > 0 {
			if err := sleepCtx(ctx, d); err != nil {
				return nil
			}
		}
		// Pick (team_id, id) coherently from v5's seeding scheme:
		// team N (where N = team_id - teamLo) seeds row IDs in
		// [startID + N*rowsPerTeam, startID + (N+1)*rowsPerTeam).
		teamID := walker.inRange(w.teamLo, w.teamHi)
		teamOffset := teamID - w.teamLo
		idLo := w.startID + teamOffset*w.rowsPerTeam
		span := w.rowsPerTeam
		if w.hotRowsPerTeam > 0 && w.hotRowsPerTeam < span {
			span = w.hotRowsPerTeam
		}
		idHi := idLo + span - 1
		id := walker.inRange(idLo, idHi)

		// Recompression-pressure payload: small but non-empty SHA-256-derived
		// blobs (same shape as the IDR loop's UPDATE: writes the three _ekm
		// columns + bumps version).
		ekm1 := sha256Bytes(id, 1)
		ekm2 := sha256Bytes(id, 2)
		ekm3 := sha256Bytes(id, 3)

		if _, err := w.write.ExecContext(ctx, stmt, ekm1, ekm2, ekm3, id, teamID); err != nil {
			if isCanceledLike(err) {
				return nil
			}
			w.counters.Errors.Add(1)
			w.logger.Warn("update error", slog.String("err", err.Error()))
			continue
		}
		w.counters.Updates.Add(1)
	}
}

// buildUpdateStmt matches the IDR run-phase UPDATE shape (SPEC §4.3) so the
// replica sees the same statement family v5 already replicates: three _ekm
// columns + version bump.
func buildUpdateStmt(schema, table string) string {
	return fmt.Sprintf(
		"UPDATE `%s`.`%s` SET contents_ekm=?, contents_highlight_ekm=?, "+
			"metadata_ekm=?, version=version+1 WHERE id=? AND team_id=?",
		schema, table,
	)
}

func sha256Bytes(seed int64, label byte) []byte {
	h := sha256.New()
	h.Write([]byte{label})
	for i := 0; i < 8; i++ {
		h.Write([]byte{byte(seed >> (8 * i))})
	}
	return h.Sum(nil)
}
