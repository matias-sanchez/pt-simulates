package runphase

import "testing"

// TestSearchMixMRRArgsMatchSeededExternalID guards the falsifier-found bug: the
// MRR IN-list values MUST match blobgen's seeded external_id format
// ("ext-<id>", id = StartID + teamIdx*RowsPerTeam + rowN) so the IN-list
// resolves real keys and MRR actually performs the clustered fetch. is_deleted
// must bind 0 (every seeded row has is_deleted=0).
func TestSearchMixMRRArgsMatchSeededExternalID(t *testing.T) {
	const startID = int64(1_000_000)
	const rowsPerTeam = int64(100)
	const teamLo = int64(463855761557)
	spec := searchSpec{
		kind: "mrr", rows: 2, teamLo: teamLo,
		startID: startID, rowsPerTeam: rowsPerTeam, dateSpan: 1000, dateLo: 1,
	}
	// Team index 1 -> teamStartID = startID + 1*rowsPerTeam = 1_000_100.
	team := teamLo + 1
	args := spec.argsFor(team, 0)
	if len(args) != 4 { // team_id, is_deleted, + 2 external_id values
		t.Fatalf("expected 4 MRR args, got %d: %v", len(args), args)
	}
	if args[0] != team {
		t.Errorf("arg0 team_id = %v, want %d", args[0], team)
	}
	if args[1] != 0 {
		t.Errorf("arg1 is_deleted = %v, want 0 (seeded rows are never deleted)", args[1])
	}
	// rowN 0 -> id = 1_000_100 -> "ext-1000100"; rowN 1 -> "ext-1000101".
	if got, want := args[2].(string), "ext-1000100"; got != want {
		t.Errorf("external_id[0] = %q, want %q (must match blobgen seed)", got, want)
	}
	if got, want := args[3].(string), "ext-1000101"; got != want {
		t.Errorf("external_id[1] = %q, want %q (must match blobgen seed)", got, want)
	}
}

// TestSearchMixRangeEstimateBindsExistingValues: range-estimate must bind
// is_deleted=0 and is_public=0 (the only seeded values) so the range matches.
func TestSearchMixRangeEstimateBindsExistingValues(t *testing.T) {
	spec := searchSpec{kind: "range_estimate", teamLo: 1, startID: 0, rowsPerTeam: 100, dateSpan: 1000, dateLo: 1}
	args := spec.argsFor(1, 3)
	if args[1] != 0 || args[2] != 0 {
		t.Errorf("is_deleted/is_public must bind 0 (seeded values); got %v, %v", args[1], args[2])
	}
}

// TestSearchMixForwardRefScanArgs: the dominant shape binds (team_id, id_cursor)
// where the cursor walks the team's seeded id range (id = startID +
// teamIdx*rowsPerTeam + rowN), so the ref scan + filesort materialises rows.
func TestSearchMixForwardRefScanArgs(t *testing.T) {
	const startID = int64(1_000_000)
	const rowsPerTeam = int64(100)
	const teamLo = int64(1_000) // arbitrary synthetic base; test exercises cursor arithmetic only
	spec := searchSpec{
		kind: "forward_refscan", rows: 10, teamLo: teamLo,
		startID: startID, rowsPerTeam: rowsPerTeam, dateSpan: 1000, dateLo: 1,
	}
	team := teamLo + 1 // teamStartID = startID + 1*rowsPerTeam = 1_000_100
	const teamStartID = int64(1_000_100)
	args := spec.argsFor(team, 5)
	if len(args) != 2 {
		t.Fatalf("expected 2 forward_refscan args (team_id, id_cursor), got %d: %v", len(args), args)
	}
	if args[0] != team {
		t.Errorf("arg0 team_id = %v, want %d", args[0], team)
	}
	// idMod = rowsPerTeam - rows = 90; n=5 -> cursor = 1_000_100 + (5 % 90) = 1_000_105.
	if got, want := args[1].(int64), int64(1_000_105); got != want {
		t.Errorf("arg1 id cursor = %d, want %d", got, want)
	}
	// The cursor must stay in the lower (rowsPerTeam - rows) band so >= `rows` ids
	// always satisfy id>cursor — the LIMIT-N clustered fetch (clustered-root
	// descent) must fire every iteration, including for large n.
	maxCursor := teamStartID + (rowsPerTeam - int64(spec.rows) - 1) // 1_000_189
	for _, nn := range []int64{0, 50, 89, 95, 1_000, 1_000_000} {
		c := spec.argsFor(team, nn)[1].(int64)
		if c < teamStartID || c > maxCursor {
			t.Errorf("n=%d cursor %d out of [%d,%d] (must leave >= rows rows above)",
				nn, c, teamStartID, maxCursor)
		}
	}
}
