// Package artifacts owns the per-run artifact directory layout described in
// SPEC §3 criterion 7 and CONSTITUTION P11: a provenance.json header
// ({commit, builtAt, configHash, hostname, startedAt, endedAt}), the
// JSON-formatted log, per-team and per-worker summary tables, and the pid
// file that `tc-idr stop` consults.
package artifacts
