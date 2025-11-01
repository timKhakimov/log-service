package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type LogStorage struct {
	cfg      Config
	db       *sql.DB
	queue    chan LogRecord
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	notifier *BotNotifier
	buffer   []LogRecord
	bufMu    sync.Mutex
}

func NewLogStorage(cfg Config, notifier *BotNotifier) *LogStorage {
	ctx, cancel := context.WithCancel(context.Background())
	s := &LogStorage{
		cfg:      cfg,
		queue:    make(chan LogRecord, cfg.MaxQueue),
		ctx:      ctx,
		cancel:   cancel,
		notifier: notifier,
		buffer:   make([]LogRecord, 0, cfg.FlushLines),
	}

	if err := s.initDB(); err != nil {
		log.Fatalf("failed to init database: %v", err)
	}

	numWorkers := 16
	if cfg.MaxQueue < 1000 {
		numWorkers = 4
	}
	for i := 0; i < numWorkers; i++ {
		s.wg.Add(1)
		go s.worker()
	}

	s.wg.Add(1)
	go s.flushLoop()

	return s
}

func (s *LogStorage) initDB() error {
	db, err := sql.Open("sqlite", s.cfg.DBPath+"?_journal=WAL&_sync=NORMAL&_cache_size=-64000")
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	s.db = db

	schema := `
	CREATE TABLE IF NOT EXISTS logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		service TEXT NOT NULL,
		level TEXT NOT NULL,
		message TEXT NOT NULL,
		metadata TEXT,
		timestamp DATETIME NOT NULL,
		received_at DATETIME NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_service_time ON logs(service, timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_timestamp ON logs(timestamp DESC);
	`

	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	return nil
}

func (s *LogStorage) Enqueue(record LogRecord) error {
	select {
	case <-s.ctx.Done():
		return errors.New("storage stopped")
	case s.queue <- record:
		return nil
	default:
		return errors.New("queue full")
	}
}

func (s *LogStorage) worker() {
	defer s.wg.Done()
	for {
		select {
		case record := <-s.queue:
			s.addToBuffer(record)
		case <-s.ctx.Done():
			s.drainQueue()
			return
		}
	}
}

func (s *LogStorage) drainQueue() {
	for {
		select {
		case record := <-s.queue:
			s.addToBuffer(record)
		default:
			s.flushBuffer()
			return
		}
	}
}

func (s *LogStorage) addToBuffer(record LogRecord) {
	s.bufMu.Lock()
	s.buffer = append(s.buffer, record)
	shouldFlush := len(s.buffer) >= s.cfg.FlushLines
	s.bufMu.Unlock()

	if shouldFlush {
		s.flushBuffer()
	}
}

func (s *LogStorage) flushLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.flushBuffer()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *LogStorage) flushBuffer() {
	s.bufMu.Lock()
	if len(s.buffer) == 0 {
		s.bufMu.Unlock()
		return
	}
	batch := make([]LogRecord, len(s.buffer))
	copy(batch, s.buffer)
	s.buffer = s.buffer[:0]
	s.bufMu.Unlock()

	if err := s.insertBatch(batch); err != nil {
		log.Printf("insert batch failed: %v", err)
		message := FormatAlert("Log Service: Batch Insert Failed", []AlertField{
			{Label: "Count", Value: fmt.Sprintf("%d", len(batch))},
			{Label: "Error", Value: err.Error()},
		})
		s.notify(message)
	}
}

func (s *LogStorage) insertBatch(records []LogRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO logs (service, level, message, metadata, timestamp, received_at) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare stmt: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
		var metadataJSON []byte
		if len(record.Metadata) > 0 {
			metadataJSON, err = json.Marshal(record.Metadata)
			if err != nil {
				log.Printf("marshal metadata failed: %v", err)
				continue
			}
		}

		_, err = stmt.Exec(
			record.Service,
			string(record.Level),
			record.Message,
			metadataJSON,
			record.Timestamp.UTC().Format(time.RFC3339Nano),
			record.ReceivedAt.UTC().Format(time.RFC3339Nano),
		)
		if err != nil {
			log.Printf("insert record failed: %v", err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func (s *LogStorage) ReadLogs(query LogQuery) ([]LogRecord, int, error) {
	var metadataFilter map[string]any
	hasFilter := false
	if query.Metadata != "" {
		if err := json.Unmarshal([]byte(query.Metadata), &metadataFilter); err != nil {
			return nil, 0, fmt.Errorf("invalid metadata JSON: %w", err)
		}
		hasFilter = len(metadataFilter) > 0
	}

	if !hasFilter {
		return s.readLogsNoFilter(query)
	}

	log.Printf("[ReadLogs] Filter mode: service=%s, filter=%v, limit=%d, offset=%d", query.Service, metadataFilter, query.Limit, query.Offset)

	selectQuery := `SELECT service, level, message, metadata, timestamp, received_at FROM logs WHERE service = ? ORDER BY timestamp DESC`
	rows, err := s.db.Query(selectQuery, query.Service)
	if err != nil {
		return nil, 0, fmt.Errorf("select query: %w", err)
	}
	defer rows.Close()

	var filteredRecords []LogRecord
	totalScanned := 0
	matchedCount := 0
	unmarshalErrors := 0
	
	for rows.Next() {
		totalScanned++
		var record LogRecord
		var metadataJSON []byte
		var timestampStr, receivedAtStr string

		err := rows.Scan(
			&record.Service,
			&record.Level,
			&record.Message,
			&metadataJSON,
			&timestampStr,
			&receivedAtStr,
		)
		if err != nil {
			log.Printf("scan row failed: %v", err)
			continue
		}

		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &record.Metadata); err != nil {
				log.Printf("unmarshal metadata failed: %v", err)
				unmarshalErrors++
				record.Metadata = make(map[string]any)
			}
		} else {
			record.Metadata = make(map[string]any)
		}

		record.Timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)
		record.ReceivedAt, _ = time.Parse(time.RFC3339Nano, receivedAtStr)
		record.RawTimestamp = timestampStr

		if matchesMetadata(record.Metadata, metadataFilter) {
			filteredRecords = append(filteredRecords, record)
			matchedCount++
		}
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("rows error: %w", err)
	}

	log.Printf("[ReadLogs] Scanned=%d, Matched=%d, UnmarshalErrors=%d", totalScanned, matchedCount, unmarshalErrors)

	total := len(filteredRecords)
	start := query.Offset
	end := query.Offset + query.Limit

	if start > total {
		log.Printf("[ReadLogs] Offset %d > Total %d, returning empty", start, total)
		return []LogRecord{}, total, nil
	}
	if end > total {
		end = total
	}

	result := filteredRecords[start:end]
	log.Printf("[ReadLogs] Returning %d records (total=%d, start=%d, end=%d)", len(result), total, start, end)
	return result, total, nil
}

func (s *LogStorage) readLogsNoFilter(query LogQuery) ([]LogRecord, int, error) {
	countQuery := `SELECT COUNT(*) FROM logs WHERE service = ?`
	var total int
	err := s.db.QueryRow(countQuery, query.Service).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("count query: %w", err)
	}

	if total == 0 {
		return []LogRecord{}, 0, nil
	}

	selectQuery := `SELECT service, level, message, metadata, timestamp, received_at FROM logs WHERE service = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`
	rows, err := s.db.Query(selectQuery, query.Service, query.Limit, query.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("select query: %w", err)
	}
	defer rows.Close()

	var allRecords []LogRecord
	for rows.Next() {
		var record LogRecord
		var metadataJSON []byte
		var timestampStr, receivedAtStr string

		err := rows.Scan(
			&record.Service,
			&record.Level,
			&record.Message,
			&metadataJSON,
			&timestampStr,
			&receivedAtStr,
		)
		if err != nil {
			log.Printf("scan row failed: %v", err)
			continue
		}

		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &record.Metadata); err != nil {
				log.Printf("unmarshal metadata failed: %v", err)
				record.Metadata = make(map[string]any)
			}
		}

		record.Timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)
		record.ReceivedAt, _ = time.Parse(time.RFC3339Nano, receivedAtStr)
		record.RawTimestamp = timestampStr

		allRecords = append(allRecords, record)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("rows error: %w", err)
	}

	return allRecords, total, nil
}

func matchesMetadata(recordMeta, filterMeta map[string]any) bool {
	if len(filterMeta) == 0 {
		return true
	}

	for key, filterValue := range filterMeta {
		recordValue, exists := recordMeta[key]
		if !exists {
			return false
		}

		switch fv := filterValue.(type) {
		case map[string]any:
			rv, ok := recordValue.(map[string]any)
			if !ok {
				return false
			}
			if !matchesMetadata(rv, fv) {
				return false
			}
		default:
			if !deepEqual(recordValue, filterValue) {
				return false
			}
		}
	}

	return true
}

func deepEqual(a, b any) bool {
	aJSON, err1 := json.Marshal(a)
	bJSON, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return string(aJSON) == string(bJSON)
}

func (s *LogStorage) GetServices() ([]string, error) {
	query := `SELECT DISTINCT service FROM logs ORDER BY service`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query services: %w", err)
	}
	defer rows.Close()

	var services []string
	for rows.Next() {
		var service string
		if err := rows.Scan(&service); err != nil {
			log.Printf("scan service failed: %v", err)
			continue
		}
		services = append(services, service)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return services, nil
}

func (s *LogStorage) Shutdown(ctx context.Context) {
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.flushBuffer()
	case <-ctx.Done():
		log.Printf("shutdown timeout, forcing close")
	}
	if s.db != nil {
		s.db.Close()
	}
}

func (s *LogStorage) notify(message string) {
	if s.notifier == nil {
		return
	}
	if strings.TrimSpace(message) == "" {
		return
	}
	s.notifier.Notify(message)
}
