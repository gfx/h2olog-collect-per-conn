CURRENT_REVISION = $(shell git rev-parse --short HEAD)

# see `go tool link -help`
BUILD_LDFLAGS = "-X main.revision=$(CURRENT_REVISION)"

H2O_REPO =  ~/ghq/github.com/h2o/h2o/
QLOG_ADAPTER = $(H2O_REPO)/deps/quicly/misc/qlog-adapter.py

CMD = h2olog-collector-gcs

all: deps build/$(CMD) build.linux-amd64/$(CMD)
.PHONY: all

build.linux-amd64/$(CMD): deps go.mod main.go
	mkdir -p build.linux-amd64
	GOOS=linux GOARCH=amd64 go build -v -o $@ -ldflags=$(BUILD_LDFLAGS)

build/$(CMD): deps go.mod main.go
	mkdir -p build
	go build -v -o $@ -ldflags=$(BUILD_LDFLAGS)

deps:
	go get -d -v
	go mod tidy -v
.PHONY: deps

update-deps:
	rm go.sum
	go get -u -v
	go mod tidy -v

test-load: build/$(CMD)
	for n in {1..20} ; \
		do echo "Testing #$n" ; \
			./build/$(CMD) -debug -local ./tmp < test/test.jsonl ; \
		done
.PHONY: test-load

test: build/$(CMD)
	./build/$(CMD) -debug -max-num-events=50 -host=test -local=./tmp < test/test.jsonl
.PHONY: test

test-qlog-adapter: test
	find tmp -name 'test-*.json' | cut --delimiter " " --fields 1 | xargs cat | jq -c '.payload[]' > tmp/test-raw.jsonl
	$(QLOG_ADAPTER) tmp/test-raw.jsonl | jq .
.PHONY: test-qlog-adapter

lint:
	go install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck

clean:
	rm -rf build build.linux-amd64 *.d
.PHONY: clean
