package main

import (
	"encoding/json"
	"errors"
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
	Service      string         `json:"service"`
	Level        LogLevel       `json:"level"`
	Message      string         `json:"message"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	Timestamp    time.Time      `json:"timestamp"`
	ReceivedAt   time.Time      `json:"receivedAt"`
	RawTimestamp string
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
		return LogRecord{}, err
	}
	
	record := LogRecord{
		Service:      r.Service,
		Level:        r.Level,
		Message:      r.Message,
		Metadata:     r.Metadata,
		ReceivedAt:   now,
		Timestamp:    parsed,
		RawTimestamp: r.Timestamp,
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

func (r LogRecord) JSON() ([]byte, error) {
	return r.StoredJSON()
}

func (r LogRecord) StoredJSON() ([]byte, error) {
	payload := map[string]any{
		"level":   r.Level,
		"message": r.Message,
	}
	if len(r.Metadata) > 0 {
		payload["metadata"] = r.Metadata
	}
	if r.RawTimestamp != "" {
		payload["timestamp"] = r.RawTimestamp
	} else {
		payload["timestamp"] = r.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	return json.Marshal(payload)
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
