package main

import (
	"bufio"
	"context"
	_ "embed"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	json "github.com/goccy/go-json"
	lru "github.com/hashicorp/golang-lru"
	"google.golang.org/api/option"
)

const numWorkers = 16
const chanBufferSize = 5000
const maxSentPn = 10
const maxAckedPn = 10
const tickDuration = 10 * time.Millisecond

var count uint64 = 0

var dryRun bool
var debug bool
var finished bool = false

var connToLogs = newLruMap(10000)

//go:embed authn.json
var authnJson []byte

var revision string

type h2ologEvent struct {
	rawEvent  map[string]interface{} // a JSON string
	createdAt time.Time
}

// value of connToLogs
type logEntry struct {
	connID  int64
	events  []h2ologEvent
	sentPn  int64 // the last packet number of "packet-sent"
	ackedPn int64 // the last packet number of "packet-acked"
}

func newLruMap(n int) *lru.Cache {
	lruMap, err := lru.New(n)
	if err != nil {
		panic(err)
	}
	return lruMap
}

func millisToTime(millis int64) time.Time {
	sec := millis / 1000
	nsec := (millis - (sec * 1000)) * 1000000
	return time.Unix(sec, nsec).UTC()
}

func clientOption() option.ClientOption {
	return option.WithCredentialsJSON(authnJson)
}

func readJSONLine(out chan []h2ologEvent, reader io.Reader) {
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := scanner.Text()
		now := time.Now()

		var rawEvent map[string]interface{}
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.UseNumber()
		err := decoder.Decode(&rawEvent)
		if err != nil {
			s := strings.TrimRight(line, "\n")
			log.Printf("Cannot parse JSON string '%s': %v", s, err)
			continue
		}

		if rawEvent["conn"] == nil {
			continue
		}

		connID, err := rawEvent["conn"].(json.Number).Int64()
		if err != nil {
			log.Fatalf("Unexpected connection ID: %v", rawEvent["conn"])
		}

		value, ok := connToLogs.Get(connID)
		var entry *logEntry
		if ok {
			entry = value.(*logEntry)
		} else {
			entry = &logEntry{
				connID:  connID,
				events:  make([]h2ologEvent, 1),
				sentPn:  -1,
				ackedPn: -1,
			}
			connToLogs.Add(connID, entry)
		}

		if entry.sentPn >= maxSentPn || entry.ackedPn >= maxAckedPn {
			continue
		}

		entry.events = append(entry.events, h2ologEvent{
			rawEvent:  rawEvent,
			createdAt: now,
		})

		eventType := rawEvent["type"]
		if eventType == "packet-sent" {
			pn, err := rawEvent["pn"].(json.Number).Int64()
			if err == nil {
				entry.sentPn = pn
			}
		} else if eventType == "packet-acked" {
			pn, err := rawEvent["pn"].(json.Number).Int64()
			if err == nil {
				entry.ackedPn = pn
			}
		}

		if eventType == "free" || entry.sentPn >= maxSentPn || entry.ackedPn >= maxAckedPn {
			log.Printf("Upload a set of events: connID=%d, type=%v, sentPn=%d, ackedPn=%d, len(events)=%d", connID, eventType, entry.sentPn, entry.ackedPn, len(entry.events))
			out <- entry.events
		}
	}
}

func uploadEvents(ctx context.Context, latch *sync.WaitGroup, in chan []h2ologEvent, bucket *storage.BucketHandle, workerID int) {
	defer func() {
		if debug {
			log.Printf("[%02d] Worker is finished", workerID)
		}
		latch.Done()
	}()

	ticker := time.NewTicker(tickDuration)
	for range ticker.C {
		select {
		case events := <-in:
			payload, err := json.Marshal(events)
			if err != nil {
				log.Printf("[%02d] Cannot serialize events: %v", err)
			}

			objectName := fmt.Sprintf("test")

			if !dryRun {
				obj := bucket.Object(objectName)
				writer := obj.NewWriter(ctx)
				writer.Write(payload)
				err := writer.Close()
				if err != nil {
					log.Printf("[%02d] Failed to write payload to %s: %v", workerID, objectName, err)
				}
			} else {
				log.Printf("[%02d][dry-run] Worker is processing events", workerID)
			}
		default:
		}

		if finished {
			break
		}
	}
}

func main() {
	var strict bool
	flag.BoolVar(&strict, "strict", false, "Turn IgnoreUnknownValues and SkipInvalidRows off")
	flag.BoolVar(&dryRun, "dry-run", false, "Do not insert values into the storage")
	flag.BoolVar(&debug, "debug", false, "Emit debug logs to STDERR")
	flag.Parse()

	if len(flag.Args()) != 1 {
		command := filepath.Base(os.Args[0])
		fmt.Printf("usage: %s [-strict] [-dry-run] [-debug] bucketID\n", command)
		os.Exit(0)
	}

	bucketID := flag.Arg(0)
	ctx := context.Background()

	client, err := storage.NewClient(ctx, clientOption())
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketID)

	ch := make(chan []h2ologEvent, chanBufferSize)
	defer close(ch)

	latch := &sync.WaitGroup{}

	for i := range make([]int, numWorkers) {
		latch.Add(1)
		go uploadEvents(ctx, latch, ch, bucket, i+1)
	}

	readJSONLine(ch, os.Stdin)
	finished = true
	latch.Wait()
}
