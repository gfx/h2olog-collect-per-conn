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
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	gcs "cloud.google.com/go/storage"
	json "github.com/goccy/go-json"
	lru "github.com/hashicorp/golang-lru"
	"google.golang.org/api/option"
)

const numWorkers = 16
const chanBufferSize = 5000
const maxSentPn = 10
const maxAckedPn = 10
const tickDuration = 10 * time.Millisecond

const capacityOfEvents = 4096 // a hint for better performance

var debug bool // -debug
var count uint64 = 0

var finished bool = false

var connToLogs = mustLruMap(10000)

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
	uploaded bool
}

type storageManager struct {
	ctx context.Context
	bucket *gcs.BucketHandle
	localDir *string
}

func (self *storageManager) write(objectName string, payload []byte) error {
	if self.localDir != nil {
		filePath := path.Join(*self.localDir, objectName + ".json")
		writer, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("Cannot create file \"%v\": %v", filePath, err)
		}
		_, err = writer.Write(payload)
		if err != nil {
			log.Fatalf("Cannot write to \"%v\": %v", filePath, err)
		}
		err = writer.Close()
		if err != nil {
			log.Fatalf("Cannot close the file \"%v\": %v", filePath, err)
		}
	}
	if self.bucket != nil {
		object := self.bucket.Object(objectName)
		writer := object.NewWriter(self.ctx)
		_, err := writer.Write(payload)
		if err != nil {
			return err
		}
		err = writer.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func mustLruMap(n int) *lru.Cache {
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
				events:  make([]h2ologEvent, 1, capacityOfEvents),
				sentPn:  -1,
				ackedPn: -1,
				uploaded: false,
			}
			connToLogs.Add(connID, entry)
		}

		if entry.uploaded {
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

		if eventType == "free" || entry.sentPn > maxSentPn || entry.ackedPn > maxAckedPn {
			if debug {
				log.Printf("[debug] Send events to workers: connID=%d, type=%v, sentPn=%d, ackedPn=%d, len(events)=%d", connID, eventType, entry.sentPn, entry.ackedPn, len(entry.events))
			}

			entry.uploaded = true
			out <- entry.events
		}
	}
}

// build a unique GCS object name from events
func buildObjectName(events []h2ologEvent) string {
	return "test" // FIXME
}

func uploadEvents(ctx context.Context, latch *sync.WaitGroup, in chan []h2ologEvent, storage *storageManager, workerID int) {
	if debug {
		log.Printf("[%02d] worker is starting", workerID)
	}
	defer func() {
		if debug {
			log.Printf("[%02d] worker is finished", workerID)
		}
		latch.Done()
	}()

	ticker := time.NewTicker(tickDuration)
	for range ticker.C {
		select {
		case events := <-in:
			payload, err := json.Marshal(events)
			if err != nil {
				log.Fatalf("[%02d] Cannot serialize events: %v", workerID, err)
			}

			objectName := buildObjectName(events)

			err = storage.write(objectName, payload)
			if err == nil {
				log.Printf("[%02d] Uploaded payload as \"%v\" (number of events = %v)", workerID, objectName, len(events))
			} else {
				log.Printf("[%02d] Failed to upload payload as \"%s\": %v", workerID, objectName, err)
			}
		default:
		}

		if finished {
			break
		}
	}
}

func main() {
	var localDir string
	var gcsBucketID string
	flag.StringVar(&localDir, "local", "", "A local directory in which it stores logs")
	flag.StringVar(&gcsBucketID, "bucket", "", "A GCS bucket ID in which it stores logs")
	flag.BoolVar(&debug, "debug", false, "Emit debug logs to STDERR")
	flag.Parse()

	if len(flag.Args()) != 0 {
		command := filepath.Base(os.Args[0])
		fmt.Printf("usage: %s [-help] [-debug] [-bucket=gcsBucketID] [-local=localDir]\n", command)
		os.Exit(0)
	}

	ctx := context.Background()

	client, err := gcs.NewClient(ctx, clientOption())
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
	}
	defer client.Close()

	storage := storageManager {
		ctx: ctx,
		bucket: nil,
		localDir: nil,
	}

	if gcsBucketID != "" {
		storage.bucket = client.Bucket(gcsBucketID)
	}

	if localDir != "" {
		os.MkdirAll(localDir, os.ModePerm)
		storage.localDir = &localDir
	}

	ch := make(chan []h2ologEvent, chanBufferSize)
	defer close(ch)

	latch := &sync.WaitGroup{}

	for i := range make([]int, numWorkers) {
		latch.Add(1)
		go uploadEvents(ctx, latch, ch, &storage, i+1)
	}

	readJSONLine(ch, os.Stdin)
	finished = true
	latch.Wait()
}
