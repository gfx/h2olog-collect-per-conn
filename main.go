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

const chanBufferSize = 5000
const maxSentPn = 10
const maxAckedPn = 10
const tickDuration = 10 * time.Millisecond

const capacityOfEvents = 4096 // a hint for better performance

var numWorkers = 16
var host string // -host=s
var debug bool // -debug
var count uint64 = 0

var finished bool = false

var connToLogs = mustLruMap(10000)

//go:embed authn.json
var authnJson []byte

//go:embed VERSION
var version string
var revision string
type h2ologEvent struct {
	rawEvent  map[string]interface{}
	createdAt time.Time // the time when this event is read by the program
}

// the schema for GCS objects
type h2ologEventRoot struct {
	Host string `json:"host"`
	StartTime time.Time `json:"start_time"`
	EndTime time.Time `json:"end_time"`
	Payload []map[string]interface{} `json:"payload"`
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

func (self *storageManager) write(objectName string, data []byte) error {
	if self.localDir != nil {
		filePath := path.Join(*self.localDir, objectName + ".json")
		err := os.WriteFile(filePath, data, os.ModePerm)
		if err != nil {
			log.Fatalf("Cannot write data to a file \"%v\": %v", filePath, err)
		}
	}
	if self.bucket != nil {
		object := self.bucket.Object(objectName)
		writer := object.NewWriter(self.ctx)
		_, err := writer.Write(data)
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
				events:  make([]h2ologEvent, 0, capacityOfEvents),
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
	// find the quicly:accept event, which probably exists in the first few events.
	for _, event := range events {
		if event.rawEvent["type"] == "accept" {
			dcid := event.rawEvent["dcid"]
			if dcid == nil {
				panic("No dcid is set in quicly:accept")
			}
			time := event.rawEvent["time"]
			if time == nil {
				panic("No time is set in quicly:accept")
			}
			return fmt.Sprintf("%s-%v-%v", host, dcid, time)
		}
	}
	panic("No quicly:accept is found in events")
}

func serializeEvents(rawEvents []h2ologEvent) ([]byte, error) {
	events := make([]map[string]interface{}, 0, len(rawEvents))
	for _, event := range rawEvents {
		events = append(events, event.rawEvent)
	}
	return json.Marshal(h2ologEventRoot {
		Host: host,
		StartTime: rawEvents[0].createdAt,
		EndTime: rawEvents[len(rawEvents) - 1].createdAt,
		Payload: events,
	})
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
			payload, err := serializeEvents(events)
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

func mustHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Cannot get hostname: %v", err)
	}
	return hostname
}

func main() {
	hostname := mustHostname()

	var localDir string
	var gcsBucketID string
	var showVersion bool
	flag.IntVar(&numWorkers, "workers", numWorkers, fmt.Sprintf("The number of workers (default: %d)", numWorkers))
	flag.StringVar(&host, "host", hostname, fmt.Sprintf("The hostname (default: %s)", hostname))
	flag.StringVar(&localDir, "local", "", "A local directory in which it stores logs")
	flag.StringVar(&gcsBucketID, "bucket", "", "A GCS bucket ID in which it stores logs")
	flag.BoolVar(&debug, "debug", false, "Emit debug logs to STDERR")
	flag.BoolVar(&showVersion, "version", false, "Show the revision and exit")
	flag.Parse()

	if (showVersion) {
		fmt.Printf("%s (rev: %s)\n", strings.TrimSpace(version), revision)
		os.Exit(0)
	}

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
