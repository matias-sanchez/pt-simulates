// Package obs sets up the dual-handler slog logger required by
// CONSTITUTION P8 and SPEC §5.5: JSON to artifacts/<run-id>/run.jsonl and
// text to stdout, with mandatory ts/level/component/phase fields on every
// record.
package obs
