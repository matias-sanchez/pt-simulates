// Package repllag samples SHOW REPLICA STATUS on a fixed interval and emits
// structured slog records. It enforces the init- and run-phase lag gates per
// SPEC-APPENDIX.md §2 R3: if Seconds_Behind_Source exceeds
// init.max_replication_lag_seconds, the orchestrator aborts the run.
package repllag
