// Package blobgen produces deterministic per-row column payloads for the
// init phase. SPEC §5.8: Generate is keyed on (teamID, rowN, seed) so two
// runs with the same JSON config produce identical wire bytes (CONSTITUTION
// P10). Statistical size parity with v4's blob_profile is required; byte
// parity with v4's Python PRNG is explicitly NOT required (SPEC C2).
package blobgen
