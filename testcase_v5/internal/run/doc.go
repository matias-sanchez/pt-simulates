// Package runphase implements the scan-encrypt-update workload described in
// SPEC §5.4 and §4.3 ("Run phase"). Per-team workers read 10-row batches
// from the replica, hash three blob columns with SHA-256, and UPDATE the
// master, advancing a keyset cursor on (date_create, id). EXPLAIN is logged
// once per worker at first batch to validate the team_id_4 index choice.
//
// The package also hosts the optional, default-off read-path diversification
// searchers (SPEC §2 Exception 2026-05-28, Track A1; see searcher.go). When
// run.search_mix is enabled they open their own replica read pool and issue
// extra read-only SELECT shapes (optimizer range-estimation, MRR execution,
// and the dominant forward ref-scan + filesort) to maximise the rate and
// diversity of the btr_cur_search_to_nth_level latch path. They never write.
//
// Directory is named "run" to match SPEC §5.1 verbatim; the Go package name
// is "runphase" to avoid colliding with the conventional `run` verb on
// command structs.
package runphase
