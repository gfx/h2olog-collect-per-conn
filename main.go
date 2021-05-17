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
	"strings"
	"sync"
	"time"

	gcs "cloud.google.com/go/storage"
	json "github.com/goccy/go-json"
	lru "github.com/hashicorp/golang-lru"
	"google.golang.org/api/option"
)

const chanBufferSize = 5000
const tickDuration = 10 * time.Millisecond

const capacityOfEvents = 4096 // a hint for better performance

var maxNumEvents int64 = 100_000 // -max-num-events
var host = mustHostname()        // -host=s
var debug bool                   // -debug
var count uint64 = 0

var connToLogs = mustLruMap(10000)

//go:embed authn.json
var authnJson []byte

//go:embed VERSION
var version string
var revision string

type h2ologEvent = map[string]interface{}

// the schema for GCS objects
type h2ologEventRoot struct {
	// metadata

	// object name
	ID string `json:"id"`
	// the guessed hostname or the one specified by -host
	Host string `json:"host"`
	// the guessed time at the time when connection started
	StartTime time.Time `json:"start_time"`
	// the guessed time at the time when connection ended
	EndTime time.Time `json:"end_time"`
	// the total number of events, may be fewer than the number of events in .payload
	NumEvents uint64 `json:"num_events"`
	// connection id
	ConnID int64 `json:"conn_id"`
	// quicly:packet_sent.pn
	SentPn int64 `json:"sent_pn"`
	// quicly:packet_acked.pn
	AckedPn int64 `json:"acked_pn"`

	// logs that h2olog emitted
	Payload []map[string]interface{} `json:"payload"`
}

// value of connToLogs
type logEntry struct {
	connID    int64
	startTime time.Time
	endTime   time.Time
	sentPn    int64 // the last packet number of "packet-sent"
	ackedPn   int64 // the last packet number of "packet-acked"
	processed bool
	numEvents uint64

	events []h2ologEvent
}

type storageManager struct {
	ctx      context.Context
	bucket   *gcs.BucketHandle
	localDir *string
}

func (self *storageManager) write(objectName string, data []byte) error {
	if self.localDir != nil {
		filePath := path.Join(*self.localDir, objectName+".json")
		err := os.WriteFile(filePath, data, os.ModePerm)
		if err != nil {
			return err
		}
	}
	if self.bucket != nil {
		object := self.bucket.Object(objectName)
		writer := object.NewWriter(self.ctx)
		writer.ContentType = "application/json; utf-8"
		_, err := writer.Write(data)
		if err != nil {
			return err
		}
		err = writer.Close()
		if err != nil {
			// TODO: handle temporary server errors
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

func readJSONLine(ctx context.Context, storage *storageManager, reader io.Reader, latch *sync.WaitGroup) {
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := scanner.Text()

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
				connID:    connID,
				startTime: time.Time{},
				endTime:   time.Time{},
				sentPn:    -1,
				ackedPn:   -1,
				processed: false,
				numEvents: 0,
				events:    make([]h2ologEvent, 0, capacityOfEvents),
			}
			connToLogs.Add(connID, entry)
		}

		if entry.processed {
			continue
		}

		timeMillis, err := rawEvent["time"].(json.Number).Int64()
		if err == nil {
			time := millisToTime(timeMillis)
			if entry.startTime.IsZero() {
				entry.startTime = time
			}

			// fill endTime with the recently-received time
			entry.endTime = time
		}

		eventType := rawEvent["type"]

		if eventType == "packet-sent" { // quicly:packet_sent
			pn, err := rawEvent["pn"].(json.Number).Int64()
			if err == nil {
				entry.sentPn = pn
			}
		} else if eventType == "packet-acked" { // quicly:packet_acked
			pn, err := rawEvent["pn"].(json.Number).Int64()
			if err == nil {
				entry.ackedPn = pn
			}
		}

		entry.numEvents++ // num skipped = entry.numEvents - len(entry.events)

		// +1 is reserved for quicly:free, which is always recorded.
		if (len(entry.events)+1) < int(maxNumEvents) || eventType == "free" {
			entry.events = append(entry.events, rawEvent)
		}

		if eventType == "free" {
			if debug {
				log.Printf("[debug] process events: living, connID=%d, type=%v, sentPn=%d, ackedPn=%d, numEvents=%d, len(events)=%d",
					connID, eventType, entry.sentPn, entry.ackedPn, entry.numEvents, len(entry.events))
			}

			entry.processed = true

			latch.Add(1)
			go uploadEvents(ctx, latch, storage, entry)
		}
	}
}

// build a unique GCS object name from events
func buildObjectName(entry *logEntry) string {
	// find the quicly:accept event, which probably exists in the first few events.
	for _, rawEvent := range entry.events {
		if rawEvent["type"] == "accept" {
			dcid := rawEvent["dcid"]
			if dcid == nil {
				panic("No dcid is set in quicly:accept")
			}
			time := rawEvent["time"]
			if time == nil {
				panic("No time is set in quicly:accept")
			}
			return fmt.Sprintf("%s-%v-%v", host, dcid, time)
		}
	}
	panic("No quicly:accept is found in events")
}

func serializeEvents(ID string, entry *logEntry) ([]byte, error) {
	rawEvents := entry.events
	return json.Marshal(h2ologEventRoot{
		ID:        ID,
		Host:      host,
		StartTime: entry.startTime,
		EndTime:   entry.endTime,
		ConnID:    entry.connID,
		SentPn:    entry.sentPn,
		AckedPn:   entry.ackedPn,
		NumEvents: entry.numEvents,
		Payload:   rawEvents,
	})
}

func uploadEvents(ctx context.Context, latch *sync.WaitGroup, storage *storageManager, entry *logEntry) {
	defer latch.Done()

	objectName := buildObjectName(entry)
	payload, err := serializeEvents(objectName, entry)
	if err != nil {
		log.Fatalf("Cannot serialize events: %v", err)
	}

	err = storage.write(objectName, payload)
	if err == nil {
		if debug {
			log.Printf("[debug] Wrote the payload as \"%v\" (events=%v, bytes=%v)",
				objectName, len(entry.events), len(payload))
		}
	} else {
		log.Printf("Failed to write the payload as \"%s\" (events=%v, bytes=%v): %v",
			objectName, len(entry.events), len(payload), err)
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
	var localDir string
	var gcsBucketID string
	var showVersion bool

	flag.Int64Var(&maxNumEvents, "max-num-events", maxNumEvents, fmt.Sprintf("Max number of events in an object (default: %v)", maxNumEvents))
	flag.StringVar(&host, "host", host, fmt.Sprintf("The hostname (default: %s)", host))
	flag.StringVar(&localDir, "local", "", "A local directory in which it stores logs")
	flag.StringVar(&gcsBucketID, "bucket", "", "A GCS bucket ID in which it stores logs")

	flag.BoolVar(&debug, "debug", false, "Emit debug logs to STDERR")
	flag.BoolVar(&showVersion, "version", false, "Show the revision and exit")
	flag.Parse()

	if showVersion {
		fmt.Printf("%s (rev: %s)\n", strings.TrimSpace(version), revision)
		os.Exit(0)
	}

	if len(flag.Args()) != 0 {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", flag.CommandLine.Name())
		flag.PrintDefaults()
		os.Exit(0)
	}

	ctx := context.Background()

	client, err := gcs.NewClient(ctx, clientOption())
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
	}
	defer client.Close()

	storage := storageManager{
		ctx:      ctx,
		bucket:   nil,
		localDir: nil,
	}

	if gcsBucketID != "" {
		storage.bucket = client.Bucket(gcsBucketID)
	}

	if localDir != "" {
		os.MkdirAll(localDir, os.ModePerm)
		storage.localDir = &localDir
	}

	latch := &sync.WaitGroup{}
	readJSONLine(ctx, &storage, os.Stdin, latch)
	latch.Wait()

	if debug {
		log.Printf("[debug] Shutting down")
	}
}
