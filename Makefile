MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
BUILD_PATH := $(dir $(MKFILE_PATH))
GOBIN ?= $(BUILD_PATH)tools/bin
LINTER_NAME := golangci-lint
LINTER_VERSION := v2.3.1
 
.PHONY: all build test bench install-linter lint

all: build

build:
	go build

test:
	go test ./... -cover

bench:
	go test -bench=. -benchmem

install-linter:
	if [ ! -f $(GOBIN)/$(LINTER_VERSION)/$(LINTER_NAME) ]; then \
		echo INSTALLING $(GOBIN)/$(LINTER_VERSION)/$(LINTER_NAME) $(LINTER_VERSION) ; \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(GOBIN)/$(LINTER_VERSION) $(LINTER_VERSION) ; \
		echo DONE ; \
	fi

lint: install-linter
	$(GOBIN)/$(LINTER_VERSION)/$(LINTER_NAME) run --config .golangci.yml