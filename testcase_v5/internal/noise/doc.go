// Package noise drives concurrent DML against the `files` table while
// the run-phase IDR scan-update loop runs.
//
// Why this exists
//
// ST-PER-003 T063 pattern analysis (tools/query-visualizer/T063-pattern-analysis.md)
// observed three workload conditions in both customer crashes:
//
//  1. Range-scan SELECT on a ROW_FORMAT=COMPRESSED table (v5 already does this).
//  2. Heavy concurrent DML on the same compressed-table family during the
//     SELECT (Apr-28: ~55k UPDATEs on the SELECT'd table + 10x surge on a
//     related table). v5 does this only weakly (the IDR UPDATEs).
//  3. Pre-crash burst: 3-4x DML rate spikes lasting ~20s, separated by ~50s
//     of baseline. Apr-28 had two such spikes inside the 90s before crash.
//
// This package adds (2) and (3) inside v5's single-table scope: concurrent
// INSERT + UPDATE workers against `files` (sized via config), with an
// optional burst schedule that periodically multiplies the rate.
//
// SPEC §2 deviation
//
// v5's SPEC §2 originally listed "No surrounding-workload mix" as a
// non-goal. The Exception added 2026-05-27 narrows that: cross-table
// noise (other keyspaces, mainuser-style traffic) remains out of scope;
// intra-table noise on `files` is in scope as a config-gated opt-in.
//
// Default behaviour
//
// Default config: noise.enabled=false. With the field absent or false the
// entire package is a no-op and the harness behaves exactly as before the
// 2026-05-27 amendment. Acceptance gates T041-T043 are unaffected.
package noise
