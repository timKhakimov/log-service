package main

import (
	"encoding/json"
	"errors"
	"log"
	"time"
)

type LogLevel string

const (
	LevelInfo  LogLevel = "info"
	LevelWarn  LogLevel = "warn"
	LevelError LogLevel = "error"
	LevelDebug LogLevel = "debug"
)

type LogRequest struct {
	Service   string         `json:"service"`
	Level     LogLevel       `json:"level"`
	Message   string         `json:"message"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Timestamp string         `json:"timestamp,omitempty"`
}

type LogRecord struct {
	Service   string         `json:"-"`
	Level     LogLevel       `json:"level"`
	Message   string         `json:"message"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Timestamp string         `json:"timestamp"`
}

func (r LogRequest) Validate() error {
	if r.Service == "" {
		return errors.New("service is required")
	}
	if r.Message == "" {
		return errors.New("message is required")
	}
	if !r.Level.Valid() {
		return errors.New("invalid level")
	}
	return nil
}

func (l LogLevel) Valid() bool {
	switch l {
	case LevelInfo, LevelWarn, LevelError, LevelDebug:
		return true
	default:
		return false
	}
}

func (r LogRequest) ToRecord(now time.Time) (LogRecord, error) {
	now = now.UTC()
	
	if r.Timestamp == "" {
		return LogRecord{}, errors.New("timestamp is required")
	}
	
	parsed, err := parseTimestamp(r.Timestamp)
	if err != nil {
		return LogRecord{}, fmt.Errorf("invalid timestamp '%s': %w", r.Timestamp, err)
	}
	
	if parsed.Year() < 2020 || parsed.Year() > 2100 {
		return LogRecord{}, fmt.Errorf("timestamp out of valid range: %s", parsed.Format(time.RFC3339))
	}
	
	record := LogRecord{
		Service:   r.Service,
		Level:     r.Level,
		Message:   r.Message,
		Metadata:  r.Metadata,
		Timestamp: parsed.Format(time.RFC3339Nano),
	}
	
	return record, nil
}

func parseTimestamp(raw string) (time.Time, error) {
	layouts := []string{time.RFC3339Nano, time.RFC3339, time.RFC1123Z, time.RFC1123}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.UTC(), nil
		}
	}
	if millis, err := time.ParseDuration(raw + "ms"); err == nil {
		return time.UnixMilli(int64(millis / time.Millisecond)).UTC(), nil
	}
	return time.Time{}, errors.New("invalid timestamp")
}

type LogQuery struct {
	Service  string
	Limit    int
	Offset   int
	Metadata string
}

type LogsResponse struct {
	Logs   []LogRecord `json:"logs"`
	Total  int         `json:"total"`
	Limit  int         `json:"limit"`
	Offset int         `json:"offset"`
}
