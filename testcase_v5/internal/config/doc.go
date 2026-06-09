// Package config loads, validates, and exposes the tc_config.json contract
// described in SPEC §4.1. It is the sole knob surface of tc-idr per
// CONSTITUTION P1 and runs fail-fast at startup per CONSTITUTION P9 — every
// validation error is collected before Load returns, and no DB connection is
// attempted until validation succeeds.
package config
