# testcase_v5 — `tc-idr` IDRBackfillHandler mimic harness

A self-contained Go harness that replays the **IDRBackfillHandler scan →
encrypt → update** workload against a MySQL/Percona Server source→replica pair,
to drive (and, on the right hardware, attempt to reproduce) the mysqld
**SIGSEGV** seen in a recurring production support case.

It is the v5 successor to the `testcase_v1`–`testcase_v4` reproduction attempts
in this repo: a single static binary instead of a Python script, ~5–10× faster
seeding, with the same workload SQL plus optional read-path diversification that
drives all three observed crash callers.

> ⚠️ **Read [Honest framing](#honest-framing) before running.** A clean run is
> **not** evidence your fleet is unaffected — this harness maximises and
> *measures* the software exposure, but the crash's root cause is believed to be
> a hardware/hypervisor fault-reporting anomaly that software alone may not be
> able to trigger.

---

## The crash this targets

`mysqld` takes signal 11 on a **replica**, during a read-only `SELECT`, with the
faulting instruction being the `endbr64` at `pthread_self+0` — reached through an
InnoDB **S-latch acquisition during a B-tree descent**:

```
btr_cur_search_to_nth_level → buf_page_get_gen → Buf_fetch::single_page
    → mtr_add_page → rw_lock_s_lock → std::this_thread::get_id → pthread_self
```

It recurs on the **clustered-index root page** of the `files` table
(`ROW_FORMAT=COMPRESSED`), via three different query callers that all converge on
that same latch path. The harness drives those three callers (see
[Reproduction soak](#reproduction-soak)).

---

## What's in here

| Path | Purpose |
|---|---|
| `cmd/tc-idr/`, `internal/`, `sql/` | the Go harness source (self-contained module) |
| `bin/tc-idr-linux-amd64` | prebuilt static linux/amd64 binary (no Go toolchain needed) |
| `Makefile` | `build` / `build-linux` / `test` / `smoke` / `lint` |
| `tc_config.json` | **template** config — the IDR loop only (fill in credentials) |
| `tc_config.search_mix.json` | **template** config — the crash-driver (3 callers) |
| `run-unattended.sh` | launch-and-leave soak: supervises `tc-idr`, watches for a crash, captures the core |
| `compose/docker-compose.yml` | containerised MySQL 8.0.36 for the local smoke test |

There are **no real credentials** in any shipped file. Every `tc_config*.json`
ships with `REPLACE_WITH_*` placeholders you fill in for your own cluster.

---

## Prerequisites

- A MySQL / Percona Server **source → replica** pair (async replication). The
  harness **reads from the replica** and **writes to the source**.
- The `byfile_tc.files` schema (`ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8`, full
  secondary-index family). The `init` phase **creates and seeds** it for you
  from `sql/schema.sql` — point the harness at an empty schema.
- To **build**: Go 1.25+. To just **run**: use `bin/tc-idr-linux-amd64`.

---

## Quick start

```bash
# 1. Build (or skip and use bin/tc-idr-linux-amd64)
make build                 # -> build/tc-idr   (current platform)
make build-linux           # -> build/tc-idr-linux-amd64

# 2. Configure: copy a template and fill in YOUR endpoints/credentials.
#    Keep the filled-in copy OUT of git (see .gitignore: tc_config.local.json).
cp tc_config.json tc_config.local.json
$EDITOR tc_config.local.json     # set database.write.* and database.read.*

# 3. (optional) small local smoke against a throwaway MySQL container
make smoke                 # needs Docker; init+run on 1 team x 100 rows

# 4. Seed the dataset (drops+creates+seeds byfile_tc.files)
./bin/tc-idr-linux-amd64 --config tc_config.local.json init

# 5. Run the IDR scan/encrypt/update loop once
./bin/tc-idr-linux-amd64 --config tc_config.local.json run
```

`tc-idr` takes exactly one flag, `--config <path>`, plus a verb:
`init` · `run` · `start` (init then run) · `status` · `tail` · `stop`.
Every other knob lives in the JSON config.

**Dataset size:** the templates default to 50 teams × 100 000 rows. Drop
`init.teams` / `init.rows_per_team` for a quick functional check; keep them high
for a faithful soak.

---

## Reproduction soak

`tc_config.search_mix.json` enables `run.search_mix`, which opens its own
read-only pool on the replica and drives the **three observed crash callers**
concurrently, flat-out, to maximise the rate and diversity of the vulnerable
latch path:

- **`forward_refscan`** — the dominant caller: a forward `team_id = const` ref
  scan (`IGNORE INDEX(PRIMARY)` + `id > ?` keyset + `ORDER BY id ASC`) → team_id
  secondary ref + filesort + clustered fetch, S-latching the clustered root.
- **`range_estimate`** — the optimizer caller: many distinct non-cached `BETWEEN`
  ranges over several candidate indexes → `records_in_range` /
  `btr_estimate_n_rows_in_range_low`.
- **`mrr`** — the MRR execution caller: a multi-value `external_id IN (...)`
  list planned `Using MRR` + clustered fetch.

Each searcher pins `REPEATABLE READ` to match the crash-time state. The block is
**off by default** in `tc_config.json`; it is **on** in
`tc_config.search_mix.json`. A soak requires `safety.max_runtime_seconds > 0`
(set it to your soak length — internal best-shot runs used `259200` = 72h).

### Launch-and-leave

```bash
TC_CONFIG=./tc_config.search_mix.local.json \
MYSQL_ERROR_LOG=/var/log/mysql/error.log \
CORE_DIR=/var/lib/mysql/cores \
./run-unattended.sh
```

The runner supervises `tc-idr`, writes a heartbeat to `run-state/STATUS`, and on
a signal-11 / new core writes `run-state/CRASH_DETECTED` and stops. It **never**
changes MySQL or system settings — it checks core-capture readiness and prints
what to fix.

### Arm core capture first (on the replica)

For a captured core to be useful, before the soak:

```sql
-- on the REPLICA: include the buffer pool in any core (the artifact prior
-- customer cores lacked), and match the crash-time state.
SET GLOBAL innodb_buffer_pool_in_core_file = ON;
```
```bash
# on the replica host: allow core dumps and route them somewhere durable
ulimit -c unlimited
cat /proc/sys/kernel/core_pattern        # ensure cores are written, not discarded
```

> 🔒 **A captured core contains live data.** With
> `innodb_buffer_pool_in_core_file=ON`, a crash core includes buffer-pool pages —
> i.e. real rows from your `files` table. Treat any core as production data and
> transfer it only under your data-sharing agreement.

---

## Honest framing

This harness is the **in-VM software** path to reproduction. Extensive testing
established two things you must carry into how you read a result:

1. **Raw workload rate is not the trigger.** The vulnerable latch path has been
   driven at >1.4M descents/second for days without a crash. More load, more
   threads, or more soak time alone do **not** make it fire.
2. **The fault looks like a hardware/hypervisor anomaly, not a software bug.**
   The kernel reports an instruction-fetch fault whose fault address equals the
   instruction pointer with a single high bit cleared — a signature that
   user-space state cannot produce. The leading candidate is a CPU
   branch-prediction erratum (Intel **ICX150**) / microcode behaviour on the
   specific host class.

Therefore:

- **A clean run is a *measured hazard-rate ceiling* + an *armed capture rig* for
  the next real event — NOT proof your fleet is unaffected.** Do not read a
  green soak as "all clear."
- The real root-cause track is **hardware**: matching the affected CPU
  microcode/host class and escalating the ICX150 / fault-report signature to the
  hardware/hypervisor vendor. This harness's most valuable output is an
  **armed, buffer-pool-included core** the moment a crash does occur.

---

## Build / test

```bash
make test          # unit tests (no Docker)
make smoke         # integration test against containerised MySQL 8.0.36 (Docker)
make lint          # golangci-lint (if installed)
```
