package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type LogStorage struct {
	cfg         Config
	queue       chan LogRecord
	writers     sync.Map
	flushTicker *time.Ticker
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	notifier    *BotNotifier
}

type fileWriter struct {
	service     string
	day         string
	path        string
	file        *os.File
	writer      *bufio.Writer
	lines       int
	flushLines  int
	durableSync time.Duration
	lastSync    time.Time
	mu          sync.Mutex
}

func NewLogStorage(cfg Config, notifier *BotNotifier) *LogStorage {
	ctx, cancel := context.WithCancel(context.Background())
	s := &LogStorage{
		cfg:         cfg,
		queue:       make(chan LogRecord, cfg.MaxQueue),
		flushTicker: time.NewTicker(cfg.FlushInterval),
		ctx:         ctx,
		cancel:      cancel,
		notifier:    notifier,
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
			s.processRecord(record)
		case <-s.ctx.Done():
			s.drainQueue()
			return
		}
	}
}

func (s *LogStorage) flushLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.flushTicker.C:
			s.flushAll()
		case <-s.ctx.Done():
			s.flushTicker.Stop()
			return
		}
	}
}

func (s *LogStorage) drainQueue() {
	for {
		select {
		case record := <-s.queue:
			s.processRecord(record)
		default:
			return
		}
	}
}

func (s *LogStorage) processRecord(record LogRecord) {
	if err := s.writeRecord(record); err != nil {
		log.Printf("write failed service=%s err=%v", record.Service, err)
		message := FormatAlert("Log Service: Write Failed", []AlertField{
			{Label: "Service", Value: record.Service},
			{Label: "Level", Value: string(record.Level)},
			{Label: "Message", Value: record.Message},
			{Label: "Error", Value: err.Error()},
		})
		s.notify(message)
	}
}

func (s *LogStorage) writeRecord(record LogRecord) error {
	payload, err := record.JSON()
	if err != nil {
		log.Printf("marshal error service=%s err=%v", record.Service, err)
		message := FormatAlert("Log Service: Marshal Error", []AlertField{
			{Label: "Service", Value: record.Service},
			{Label: "Message", Value: record.Message},
			{Label: "Error", Value: err.Error()},
		})
		s.notify(message)
		return err
	}
	maxAttempts := 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := s.writeOnce(record, payload); err != nil {
			lastErr = err
			if attempt < maxAttempts {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			break
		}
		return nil
	}
	return lastErr
}

func (s *LogStorage) writeOnce(record LogRecord, payload []byte) error {
	writer, err := s.getWriter(record)
	if err != nil {
		return err
	}
	if err := writer.write(payload, s.cfg.FlushLines); err != nil {
		s.removeWriter(writer.service, writer.day)
		return err
	}
	return nil
}

func (s *LogStorage) getWriter(record LogRecord) (*fileWriter, error) {
	service := sanitizeService(record.Service)
	day := record.Timestamp.Format("2006-01-02")
	key := writerKey(service, day)
	if val, ok := s.writers.Load(key); ok {
		return val.(*fileWriter), nil
	}
	if err := os.MkdirAll(filepath.Join(s.cfg.LogDir, service), 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(s.cfg.LogDir, service, fmt.Sprintf("%s.ndjson", day))
	fw, err := openFileWriter(service, day, path, s.cfg.FlushLines, s.cfg.DurableSync)
	if err != nil {
		return nil, err
	}
	actual, loaded := s.writers.LoadOrStore(key, fw)
	if loaded {
		fw.close()
		return actual.(*fileWriter), nil
	}
	go s.cleanupOldWriters(service, day, key)
	return fw, nil
}

func (s *LogStorage) cleanupOldWriters(service, day, keepKey string) {
	s.writers.Range(func(key, value interface{}) bool {
		k := key.(string)
		if k == keepKey {
			return true
		}
		parts := strings.SplitN(k, "|", 2)
		if len(parts) != 2 {
			return true
		}
		if parts[0] == service && parts[1] != day {
			w := value.(*fileWriter)
			if err := w.flush(); err != nil {
				log.Printf("flush error key=%s err=%v", k, err)
				message := FormatAlert("Log Service: Flush Error", []AlertField{
					{Label: "Service", Value: service},
					{Label: "Day", Value: parts[1]},
					{Label: "Error", Value: err.Error()},
				})
				s.notify(message)
			}
			w.close()
			s.writers.Delete(k)
		}
		return true
	})
}

func (s *LogStorage) removeWriter(service, day string) {
	key := writerKey(service, day)
	if val, ok := s.writers.LoadAndDelete(key); ok {
		val.(*fileWriter).close()
	}
}

func (s *LogStorage) flushAll() {
	s.writers.Range(func(key, value interface{}) bool {
		w := value.(*fileWriter)
		if err := w.flush(); err != nil {
			k := key.(string)
			parts := strings.SplitN(k, "|", 2)
			service := parts[0]
			day := ""
			if len(parts) > 1 {
				day = parts[1]
			}
			log.Printf("flush error key=%s err=%v", k, err)
			message := FormatAlert("Log Service: Flush Error", []AlertField{
				{Label: "Service", Value: service},
				{Label: "Day", Value: day},
				{Label: "Error", Value: err.Error()},
			})
			s.notify(message)
		}
		return true
	})
}

func (s *LogStorage) closeAll() {
	s.writers.Range(func(key, value interface{}) bool {
		w := value.(*fileWriter)
		if err := w.flush(); err != nil {
			k := key.(string)
			parts := strings.SplitN(k, "|", 2)
			service := parts[0]
			day := ""
			if len(parts) > 1 {
				day = parts[1]
			}
			log.Printf("flush error key=%s err=%v", k, err)
			message := FormatAlert("Log Service: Flush Error", []AlertField{
				{Label: "Service", Value: service},
				{Label: "Day", Value: day},
				{Label: "Error", Value: err.Error()},
			})
			s.notify(message)
		}
		w.close()
		s.writers.Delete(key)
		return true
	})
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
		s.flushAll()
		s.closeAll()
	case <-ctx.Done():
		log.Printf("shutdown timeout, forcing close")
		s.closeAll()
	}
}

func openFileWriter(service, day, path string, flushLines int, durable time.Duration) (*fileWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &fileWriter{
		service:     service,
		day:         day,
		path:        path,
		file:        file,
		writer:      bufio.NewWriterSize(file, 64*1024),
		flushLines:  flushLines,
		durableSync: durable,
		lastSync:    time.Now(),
	}, nil
}

func (w *fileWriter) write(payload []byte, flushLines int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer == nil {
		if err := w.reopen(); err != nil {
			return err
		}
	}
	if _, err := w.writer.Write(payload); err != nil {
		w.closeLocked()
		return err
	}
	if err := w.writer.WriteByte('\n'); err != nil {
		w.closeLocked()
		return err
	}
	w.lines++
	if flushLines > 0 && w.lines >= flushLines {
		if err := w.flushLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (w *fileWriter) reopen() error {
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	w.writer = bufio.NewWriterSize(file, 64*1024)
	return nil
}

func (w *fileWriter) flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

func (w *fileWriter) flushLocked() error {
	if w.writer == nil {
		return nil
	}
	if err := w.writer.Flush(); err != nil {
		w.closeLocked()
		return err
	}
	w.lines = 0
	if w.durableSync > 0 {
		if time.Since(w.lastSync) >= w.durableSync {
			if err := w.file.Sync(); err != nil {
				w.closeLocked()
				return err
			}
			w.lastSync = time.Now()
		}
	}
	return nil
}

func (w *fileWriter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeLocked()
}

func (w *fileWriter) closeLocked() {
	if w.writer != nil {
		w.writer.Flush()
	}
	if w.file != nil {
		w.file.Close()
	}
	w.writer = nil
	w.file = nil
}

func sanitizeService(service string) string {
	if service == "" {
		return "default"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", "..", "_", " ", "-")
	return replacer.Replace(service)
}

func writerKey(service, day string) string {
	return service + "|" + day
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

func (s *LogStorage) ReadLogs(query LogQuery) ([]LogRecord, int, error) {	
	serviceDir := filepath.Join(s.cfg.LogDir, sanitizeService(query.Service))
	pattern := filepath.Join(serviceDir, "*.ndjson")

	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, 0, err
	}

	if len(files) == 0 {
		return []LogRecord{}, 0, nil
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i] > files[j]
	})

	var metadataFilter map[string]any
	if query.Metadata != "" {
		if err := json.Unmarshal([]byte(query.Metadata), &metadataFilter); err != nil {
			return nil, 0, fmt.Errorf("invalid metadata JSON: %w", err)
		}
	}

	var allMatchingLogs []LogRecord

	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			continue
		}

		var lines [][]byte
		scanner := bufio.NewScanner(file)
		
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 100*1024*1024)
		
		for scanner.Scan() {
			line := make([]byte, len(scanner.Bytes()))
			copy(line, scanner.Bytes())
			lines = append(lines, line)
		}
		
		if err := scanner.Err(); err != nil {
			file.Close()
			message := FormatAlert("Log Service: Scanner Error", []AlertField{
				{Label: "File", Value: filePath},
				{Label: "Service", Value: query.Service},
				{Label: "Error", Value: err.Error()},
			})
			s.notify(message)
			continue
		}
		
		file.Close()

		for i := len(lines) - 1; i >= 0; i-- {
			line := lines[i]
			if len(line) == 0 {
				continue
			}

			var record LogRecord
			if err := json.Unmarshal(line, &record); err != nil {
				continue
			}

			if !matchesMetadata(record.Metadata, metadataFilter) {
				continue
			}

			allMatchingLogs = append(allMatchingLogs, record)
		}
	}

	total := len(allMatchingLogs)

	start := query.Offset
	if start >= total {
		return []LogRecord{}, total, nil
	}

	end := start + query.Limit
	if end > total {
		end = total
	}

	result := allMatchingLogs[start:end]
	return result, total, nil
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

		if !deepEqual(recordValue, filterValue) {
			return false
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
	entries, err := os.ReadDir(s.cfg.LogDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var services []string
	for _, entry := range entries {
		if entry.IsDir() {
			services = append(services, entry.Name())
		}
	}

	sort.Strings(services)
	return services, nil
}
