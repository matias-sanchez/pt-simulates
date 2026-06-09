package runphase

import (
	"strings"
	"testing"

	"github.com/percona-cs/cs0055422-tc-idr/internal/blobgen"
)

func TestSelectSQLShape(t *testing.T) {
	got := selectSQL("byfile_tc", "files", 10)
	for _, want := range []string{
		"FROM `byfile_tc`.`files`",
		"WHERE team_id = ? AND date_create > ?",
		"ORDER BY date_create ASC, id ASC",
		"LIMIT 10",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("SELECT missing %q: %s", want, got)
		}
	}
	// Sanity check: every column in the SELECT list is also in
	// blobgen.Columns() (and vice versa) — wire shape parity with v4.
	for _, c := range blobgen.Columns() {
		if !strings.Contains(got, "`"+c+"`") {
			t.Errorf("SELECT missing column %q", c)
		}
	}
}

func TestUpdateSQLShape(t *testing.T) {
	got := updateSQL("byfile_tc", "files")
	for _, want := range []string{
		"UPDATE `byfile_tc`.`files`",
		"SET contents_ekm = ?",
		"contents_highlight_ekm = ?",
		"metadata_ekm = ?",
		"version = version + 1",
		"WHERE id = ? AND team_id = ?",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("UPDATE missing %q: %s", want, got)
		}
	}
}

func TestEncryptBlobDeterministic(t *testing.T) {
	a := encryptBlob([]byte("hello"), "label-1")
	b := encryptBlob([]byte("hello"), "label-1")
	if string(a) != string(b) {
		t.Fatal("encryption not deterministic")
	}
	c := encryptBlob([]byte("hello"), "label-2")
	if string(a) == string(c) {
		t.Fatal("different labels must produce different ciphertext")
	}
	if got, want := len(encryptBlob(nil, "x")), 32; got != want {
		t.Fatalf("empty source must yield 32 bytes; got %d", got)
	}
}

// TestSelectSQLParity pins the frozen IDR loop shape byte-for-byte: the
// search-mix refactor (shared columnList) must not change selectSQL output.
func TestSelectSQLParity(t *testing.T) {
	want := "SELECT " + columnList() + " FROM `byfile_tc`.`files` " +
		"WHERE team_id = ? AND date_create > ? ORDER BY date_create ASC, id ASC LIMIT 10"
	if got := selectSQL("byfile_tc", "files", 10); got != want {
		t.Errorf("selectSQL drifted:\n got: %s\nwant: %s", got, want)
	}
}

// TestRangeEstimateSQLShape: the optimizer-side caller (records_in_range over
// multiple candidate indexes).
func TestRangeEstimateSQLShape(t *testing.T) {
	got := rangeEstimateSQL("byfile_tc", "files", 10)
	for _, want := range []string{
		"FROM `byfile_tc`.`files`",
		"WHERE team_id = ? AND is_deleted = ? AND is_public = ? AND date_create BETWEEN ? AND ?",
		"LIMIT 10",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("range-estimate SELECT missing %q: %s", want, got)
		}
	}
	for _, c := range blobgen.Columns() {
		if !strings.Contains(got, "`"+c+"`") {
			t.Errorf("range-estimate SELECT missing column %q", c)
		}
	}
}

// TestMrrSQLShape: the MRR execution caller. Placeholder count must match the
// requested IN-list width.
func TestMrrSQLShape(t *testing.T) {
	got := mrrSQL("byfile_tc", "files", 3)
	for _, want := range []string{
		"WHERE team_id = ? AND is_deleted = ? AND `external_id` IN (?, ?, ?)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("mrr SELECT missing %q: %s", want, got)
		}
	}
	if got1 := mrrSQL("byfile_tc", "files", 0); !strings.Contains(got1, "IN (?)") {
		t.Errorf("mrr SELECT with inCount<1 must clamp to one placeholder: %s", got1)
	}
}

// TestForwardRefScanSQLShape: the DOMINANT caller (forward team_id ref scan +
// filesort -> clustered ROOT). IGNORE INDEX(PRIMARY) + id-cursor + ORDER BY id.
func TestForwardRefScanSQLShape(t *testing.T) {
	got := forwardRefScanSQL("byfile_tc", "files", 7)
	for _, want := range []string{
		"FROM `byfile_tc`.`files` IGNORE INDEX (`PRIMARY`)",
		"WHERE team_id = ? AND id > ?",
		"ORDER BY id ASC",
		"LIMIT 7",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("forward_refscan SELECT missing %q: %s", want, got)
		}
	}
	// Must NOT pin the clustered PK (that path stops after LIMIT without the
	// filesort/clustered-fetch the fingerprint requires).
	if strings.Contains(got, "USE INDEX") || strings.Contains(got, "ORDER BY date_create") {
		t.Errorf("forward_refscan must use IGNORE INDEX(PRIMARY) + ORDER BY id: %s", got)
	}
	for _, c := range blobgen.Columns() {
		if !strings.Contains(got, "`"+c+"`") {
			t.Errorf("forward_refscan SELECT missing column %q", c)
		}
	}
}
