package config

// Config mirrors tc_config.json (SPEC §4.1). Field shape is drop-in
// compatible with v4's tc_config.json (SPEC C7) plus three v5-only
// additions: artifacts, init.max_replication_lag_seconds, debug.log_sql_once
// (SPEC C13).
//
// Fields that v4 used but v5 ignores at runtime (e.g. remote.screen_sessions)
// are kept here so DisallowUnknownFields stays strict; v5 logs a warning on
// startup that they are inert.
type Config struct {
	Remote    Remote    `json:"remote"`
	Database  Database  `json:"database"`
	Init      Init      `json:"init"`
	Run       Run       `json:"run"`
	Noise     Noise     `json:"noise"`
	Safety    Safety    `json:"safety"`
	Artifacts Artifacts `json:"artifacts"`
	Debug     Debug     `json:"debug"`
}

// Noise carries the optional sibling-DML workers that run alongside the
// IDR scan-update loop. SPEC §2 Exception (2026-05-27): intra-table noise
// on `files` is in scope as a config-gated opt-in; cross-table workloads
// remain out of scope. See internal/noise/doc.go + T063-pattern-analysis.md.
type Noise struct {
	Enabled            bool      `json:"enabled"`
	InsertWorkers      int       `json:"insert_workers"`
	InsertRatePerSec   int       `json:"insert_rate_per_sec"`
	InsertStartID      int64     `json:"insert_start_id"`
	UpdateWorkers      int       `json:"update_workers"`
	UpdateRatePerSec   int       `json:"update_rate_per_sec"`
	// UpdateHotRowsPerTeam restricts UPDATE noise to the first N rows of
	// each team's seeded range. Concentrates same-page contention to amplify
	// the compressed-page recompression pressure observed in the Apr-28
	// pattern. 0 = use full per-team range.
	UpdateHotRowsPerTeam int       `json:"update_hot_rows_per_team"`
	// PoolMaxConns overrides the noise-side write pool size. 0 = falls back
	// to the IDR run pool size. Set higher (e.g. update_workers + insert_workers
	// + 4) to avoid contention with the IDR loop.
	PoolMaxConns       int       `json:"pool_max_conns"`
	LogIntervalSeconds int       `json:"log_interval_seconds"`
	Burst              BurstCfg  `json:"burst"`
}

// BurstCfg is the time-shaped DML rate multiplier matching the Apr-28
// crash-window pattern (~20s spike + ~50s baseline, 3-4x multiplier).
type BurstCfg struct {
	Enabled         bool    `json:"enabled"`
	Multiplier      float64 `json:"multiplier"`
	BurstSeconds    int     `json:"burst_seconds"`
	IntervalSeconds int     `json:"interval_seconds"`
}

// Remote captures the v4 remote.* block. v5 ignores every field at runtime
// (sysbench-style invocation per SPEC C5) but parses them to keep the JSON
// shape v4-compatible.
type Remote struct {
	RemoteDir      string            `json:"remote_dir"`
	LogsDir        string            `json:"logs_dir"`
	ScreenSessions map[string]string `json:"screen_sessions"`
}

// Database wraps the two endpoint definitions. SPEC §0.4: write is a
// local Unix socket on slack-master; read is the replica TCP endpoint.
type Database struct {
	Write Endpoint `json:"write"`
	Read  Endpoint `json:"read"`
}

// Endpoint captures one side of the master/replica pair. Either Host or
// Socket must be set; Socket wins when both are present.
type Endpoint struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	Socket   string `json:"socket"`
}

// Init carries the seeding knobs from SPEC §4.1.
type Init struct {
	Teams                     int         `json:"teams"`
	RowsPerTeam               int         `json:"rows_per_team"`
	TeamIDBase                int64       `json:"team_id_base"`
	StartID                   int64       `json:"start_id"`
	DateBase                  int64       `json:"date_base"`
	DateStep                  int64       `json:"date_step"`
	Seed                      uint64      `json:"seed"`
	BlobProfile               BlobProfile `json:"blob_profile"`
	InsertBatchRows           int         `json:"insert_batch_rows"`
	InsertBatchBytes          int         `json:"insert_batch_bytes"`
	InitProgressRows          int         `json:"init_progress_rows"`
	TeamProgressRows          int         `json:"team_progress_rows"`
	InitWorkers               int         `json:"init_workers"`
	MaxParallelTeams          int         `json:"max_parallel_teams"`
	RelaxDurability           bool        `json:"relax_durability"`
	ForceInit                 bool        `json:"force_init"`
	MaxReplicationLagSeconds  int         `json:"max_replication_lag_seconds"`
	// Opt-in performance levers — default off to preserve SPEC §4.3 wire
	// shape. Each one is documented in the per-field comments below.
	//
	// DisableRedoLog: ALTER INSTANCE DISABLE INNODB REDO_LOG before init,
	// re-enable after. Skips redo log writes entirely. CRITICAL: a server
	// crash during init leaves InnoDB unrecoverable — only enable on
	// throw-away test clusters where force_init can be replayed.
	DisableRedoLog            bool        `json:"disable_redo_log"`
	// BulkIndexRebuild: create the table with PRIMARY KEY only, seed all
	// rows, then ALTER TABLE ADD KEY for every secondary index parsed
	// out of the embedded schema. End-state matches SPEC C8 byte-identical
	// schema; intermediate timing differs (replica sees DDL events).
	BulkIndexRebuild          bool        `json:"bulk_index_rebuild"`
}

// BlobProfile is the size-class mix from SPEC §4.1; percentages must sum to
// 100 (enforced by Validate).
type BlobProfile struct {
	SmallPct  int `json:"small_pct"`
	MediumPct int `json:"medium_pct"`
	LargePct  int `json:"large_pct"`
}

// Run carries the workload knobs from SPEC §4.1.
type Run struct {
	BatchSize                      int              `json:"batch_size"`
	ReadSource                     string           `json:"read_source"`
	WriteTarget                    string           `json:"write_target"`
	Encryption                     Encryption       `json:"encryption"`
	StopCondition                  StopCondition    `json:"stop_condition"`
	ReplicationCheck               ReplicationCheck `json:"replication_check"`
	MaxParallelTeams               int              `json:"max_parallel_teams"`
	ProgressIntervalBatches        int              `json:"progress_interval_batches"`
	TeamLogIntervalBatches         int              `json:"team_log_interval_batches"`
	ReplicationLogIntervalSeconds  int              `json:"replication_log_interval_seconds"`
	SearchMix                      SearchMix        `json:"search_mix"`
}

// SearchMix carries the optional read-path diversification searchers that run
// alongside the IDR scan-update loop on the replica. SPEC §2 Exception
// (2026-05-28, Track A1): diversifying the *read* access paths on
// `byfile_tc.files` is in scope as a config-gated opt-in so the harness drives
// the observed crash callers — optimizer range-estimation, MRR execution, and
// the DOMINANT forward `team_id = const` ref scan + filesort (cores
// oct25/oct27a/oct27b, F-022) — which all converge on
// btr_cur_search_to_nth_level -> buf_page_get_gen -> single_page ->
// rw_lock_s_lock_low. (The earlier backward `ha_index_prev` shape was replaced:
// it appears in NO core — STATUS NEXT-ACTION #0.) Searchers are read-only.
// Default Enabled=false is a no-op and preserves SPEC §4.3 wire shape + the
// T041–T043 gates bit-for-bit. See internal/run/searcher.go + SPEC-APPENDIX §7.2.
type SearchMix struct {
	Enabled bool `json:"enabled"`
	// RangeEstimateWorkers issue many distinct non-cached BETWEEN ranges over
	// multiple candidate indexes -> records_in_range /
	// btr_estimate_n_rows_in_range_low (the optimizer-side crash caller).
	RangeEstimateWorkers int `json:"range_estimate_workers"`
	// MRRWorkers issue a multi-value external_id IN-list over the team_id_5 /
	// team_id_8 secondary indexes, planned "Using MRR" + clustered fetch ->
	// ha_multi_range_read_next. The searcher sets optimizer_switch
	// 'mrr=on,mrr_cost_based=off' on its own read session to force the plan.
	MRRWorkers int `json:"mrr_workers"`
	// ForwardRefScanWorkers issue the DOMINANT shape: a forward `team_id = const`
	// ref scan with `IGNORE INDEX(PRIMARY)` + an `id > ?` keyset cursor +
	// `ORDER BY id ASC` -> a team_id secondary ref (ha_index_next_same /
	// RefIterator) + filesort + clustered fetch, S-latching the clustered ROOT
	// (page 4). This is the canonical IDRBackfillHandler crash query (oct27a).
	ForwardRefScanWorkers int `json:"forward_refscan_workers"`
	// RowsPerQuery bounds each searcher query (LIMIT, and the IN-list width for
	// MRR). 0 falls back to the SPEC LIMIT of 10.
	RowsPerQuery int `json:"rows_per_query"`
	// LogIntervalSeconds throttles per-shape progress logs. 0 falls back to 60.
	LogIntervalSeconds int `json:"log_interval_seconds"`
}

// Encryption captures the run.encryption block. SPEC §4.3 fixes Mode to
// "sha256"; Validate refuses any other value.
type Encryption struct {
	Mode string `json:"mode"`
}

// StopCondition captures run.stop_condition. Only "all_rows" is in scope
// (SPEC §2).
type StopCondition struct {
	Mode string `json:"mode"`
}

// ReplicationCheck captures run.replication_check. Only
// "show_replica_status_only" is in scope.
type ReplicationCheck struct {
	Mode string `json:"mode"`
}

// Safety carries the run-wide safety net knobs. MaxRuntimeSeconds=0 means
// "no deadline" (mirrors v4's null).
type Safety struct {
	ForceInitRequiresFlag bool `json:"force_init_requires_flag"`
	MaxRuntimeSeconds     int  `json:"max_runtime_seconds"`
}

// Artifacts carries the per-run artifact directory (SPEC C13).
type Artifacts struct {
	Dir string `json:"dir"`
}

// Debug carries the wire-level debugging toggle (SPEC-APPENDIX.md §2 R4 / §5 C13).
type Debug struct {
	LogSQLOnce bool `json:"log_sql_once"`
}
