CURRENT_REVISION = $(shell git rev-parse --short HEAD)

# see `go tool link -help`
BUILD_LDFLAGS = "-X main.revision=$(CURRENT_REVISION)"

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
.PHONY: test

test: build/$(CMD)
	./build/$(CMD) -debug -host foo -local ./tmp < test/test.jsonl
.PHONY: test-single-run

clean:
	rm -rf build build.linux-amd64 *.d
.PHONY: clean
