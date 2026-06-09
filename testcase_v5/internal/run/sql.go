package runphase

import (
	"fmt"
	"strings"

	"github.com/percona-cs/cs0055422-tc-idr/internal/blobgen"
)

// columnList renders the 43 backtick-quoted column names in blobgen.Columns()
// order, comma-separated. Shared by every read shape so the SELECT projection
// stays byte-identical to the v4 wire shape (SPEC §4.3).
func columnList() string {
	cols := blobgen.Columns()
	var sb strings.Builder
	for i, c := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('`')
		sb.WriteString(c)
		sb.WriteByte('`')
	}
	return sb.String()
}

// selectSQL renders the 43-column SELECT statement from v4 run.py / SPEC
// §4.3. Order matches blobgen.Columns(); the LIMIT is the SPEC-mandated
// run.batch_size==10 (config validator enforces). This is the frozen IDR
// loop shape and MUST NOT change (default-off parity guard).
func selectSQL(schema, table string, batchSize int) string {
	return fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE team_id = ? AND date_create > ? "+
			"ORDER BY date_create ASC, id ASC LIMIT %d",
		columnList(), schema, table, batchSize)
}

// rangeEstimateSQL drives the optimizer-side crash caller (SPEC §2 Exception
// 2026-05-28). Predicates that several secondary indexes (team_id_2/3/6/7) can
// serve force the optimizer to run records_in_range ->
// btr_estimate_n_rows_in_range_low across candidate indexes at plan time.
// Bind order: team_id, is_deleted, is_public, date_create_lo, date_create_hi.
func rangeEstimateSQL(schema, table string, batchSize int) string {
	return fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE team_id = ? AND is_deleted = ? "+
			"AND is_public = ? AND date_create BETWEEN ? AND ? LIMIT %d",
		columnList(), schema, table, batchSize)
}

// mrrSQL drives the MRR execution caller. A multi-value external_id IN-list
// scoped by (team_id, is_deleted) resolves through the team_id_5 secondary
// index then fetches the clustered rows — planned "Using MRR" when the searcher
// sets optimizer_switch='mrr=on,mrr_cost_based=off' on its session. Bind order:
// team_id, is_deleted, then inCount external_id values.
func mrrSQL(schema, table string, inCount int) string {
	if inCount < 1 {
		inCount = 1
	}
	ph := strings.TrimSuffix(strings.Repeat("?, ", inCount), ", ")
	return fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` WHERE team_id = ? AND is_deleted = ? "+
			"AND `external_id` IN (%s)",
		columnList(), schema, table, ph)
}

// forwardRefScanSQL drives the DOMINANT observed crash caller (catalog cores
// oct25/oct27a/oct27b, F-022; the canonical IDRBackfillHandler shape). A forward
// `team_id = const` ref scan whose `IGNORE INDEX(PRIMARY)` pushes the optimizer
// off the clustered PK onto a team_id secondary (team_id_4 family); the `id > ?`
// keyset cursor + `ORDER BY id ASC` then forces a filesort because no team_id
// secondary is id-ordered. The 43-column projection forces a clustered fetch per
// row, so the B-tree descent S-latches the clustered-index ROOT (page 4,
// index_name=PRIMARY) — the read path identical across all execution-ref cores,
// via ha_index_next_same / RefIterator. Bind order: team_id, id_cursor.
//
// NOTE: faithful to the recovered query, which uses IGNORE INDEX(PRIMARY) only.
// If the local-smoke EXPLAIN gate shows the optimizer choosing a full scan rather
// than a team_id secondary, add FORCE INDEX(team_id_4) here to pin the documented
// index (SPEC §4.3) — the InnoDB descent is unchanged either way.
func forwardRefScanSQL(schema, table string, batchSize int) string {
	return fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` IGNORE INDEX (`PRIMARY`) "+
			"WHERE team_id = ? AND id > ? ORDER BY id ASC LIMIT %d",
		columnList(), schema, table, batchSize)
}

// updateSQL renders the per-row UPDATE statement from SPEC §4.3.
func updateSQL(schema, table string) string {
	return fmt.Sprintf(
		"UPDATE `%s`.`%s` SET contents_ekm = ?, contents_highlight_ekm = ?, "+
			"metadata_ekm = ?, version = version + 1 "+
			"WHERE id = ? AND team_id = ?", schema, table)
}
