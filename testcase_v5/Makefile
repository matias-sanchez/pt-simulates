# Governed by CONSTITUTION.md (P11, P12) and SPEC.md §6.
# Build and deploy convenience only; no runtime orchestration.

SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

MODULE      := github.com/percona-cs/cs0055422-tc-idr
BIN_NAME    := tc-idr
BUILD_DIR   := build
LINUX_BIN   := $(BUILD_DIR)/$(BIN_NAME)-linux-amd64
DEPLOY_HOST := slack-master
DEPLOY_PATH := /mnt/vitess/testcase/tc_v5/$(BIN_NAME)
SCHEMA_V4   := ../../_v4/tc_idrbackfillhandler_mimic/sql/schema.sql
SCHEMA_V5   := sql/schema.sql
SCHEMA_EMB  := internal/schema/embedded.sql

# --verify exits silently on an unborn HEAD (no output on stdout). Fall back
# to "unknown" via Make's ifeq guard so the ldflags args stay well-formed
# (no whitespace inside the commit value).
COMMIT     := $(shell git rev-parse --verify HEAD 2>/dev/null)
ifeq ($(COMMIT),)
COMMIT := unknown
endif
BUILT_AT   := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS    := -s -w -X main.commit=$(COMMIT) -X main.builtAt=$(BUILT_AT)

.PHONY: all schema-check schema-sync build build-linux test lint vet deploy clean smoke smoke-up smoke-down line-count

all: build

# Verifies the v4 schema, the v5 canonical schema, and the embedded copy are
# all byte-identical (CONSTITUTION P5 + SPEC C8). The embedded copy must
# live inside internal/schema/ because go:embed forbids escaping the package
# directory; this target keeps it in sync with sql/schema.sql.
schema-check:
	@cmp $(SCHEMA_V4) $(SCHEMA_V5) || { \
		echo "v5 sql/schema.sql diverges from v4; rerun: cp $(SCHEMA_V4) $(SCHEMA_V5)"; \
		exit 1; \
	}
	@cmp $(SCHEMA_V5) $(SCHEMA_EMB) || { \
		echo "embedded.sql diverges from sql/schema.sql; rerun: make schema-sync"; \
		exit 1; \
	}

schema-sync:
	cp $(SCHEMA_V5) $(SCHEMA_EMB)

build: schema-check
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BUILD_DIR)/$(BIN_NAME) ./cmd/$(BIN_NAME)

build-linux: schema-check
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
		go build -trimpath -ldflags="$(LDFLAGS)" -o $(LINUX_BIN) ./cmd/$(BIN_NAME)

test: schema-check
	go test ./...

line-count:
	@../../../scripts/check-line-count.sh

lint: line-count schema-check
	golangci-lint run

vet:
	go vet ./...

# Deploys the linux/amd64 binary to slack-master at the v5 path (SPEC §0.3).
# SSH alias `slack-master` is configured in maintainer's ~/.ssh/config.
deploy: build-linux
	rsync -av --progress $(LINUX_BIN) $(DEPLOY_HOST):$(DEPLOY_PATH)

clean:
	rm -rf $(BUILD_DIR)

# Integration smoke: bring up containerized MySQL, run the tagged tests,
# tear down. Requires Docker (see compose/docker-compose.yml).
smoke: smoke-up
	go test -tags=integration -timeout 5m ./internal/smoke/... || { \
		$(MAKE) smoke-down; \
		exit 1; \
	}
	$(MAKE) smoke-down

smoke-up:
	docker compose -f compose/docker-compose.yml up -d

smoke-down:
	docker compose -f compose/docker-compose.yml down -v
