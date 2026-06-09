// Command tc-idr replays the IDRBackfillHandler scan-encrypt-update workload
// against the cs0055422 reproduction cluster. SPEC §1: target is 5–10× the
// v4 Python harness on init throughput, with identical SQL semantics so the
// replica-side reproduction conditions are unchanged.
//
// CLI surface (CONSTITUTION P1): exactly one flag, --config <path>, plus a
// positional verb (init|run|start|status|tail|stop). All runtime knobs live
// in tc_config.json.
package main

import (
	"fmt"
	"os"
)

// commit and builtAt are injected at build time via -ldflags (Makefile
// build-linux target / CONSTITUTION P11). Defaults make local `go run` and
// `go test` builds explicit about their provenance instead of silently
// reporting empty strings.
var (
	commit  = "dev"
	builtAt = "unknown"
)

func main() {
	if err := newRootCmd(commit, builtAt).Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
