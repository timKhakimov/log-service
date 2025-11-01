package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogStorage struct {
	cfg        Config
	client     *mongo.Client
	collection *mongo.Collection
	queue      chan LogRecord
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	notifier   *BotNotifier
}

func NewLogStorage(cfg Config, notifier *BotNotifier) *LogStorage {
	ctx, cancel := context.WithCancel(context.Background())
	s := &LogStorage{
		cfg:      cfg,
		queue:    make(chan LogRecord, cfg.MaxQueue),
		ctx:      ctx,
		cancel:   cancel,
		notifier: notifier,
	}

	if err := s.initDB(); err != nil {
		log.Fatalf("failed to init database: %v", err)
	}

	s.wg.Add(1)
	go s.batchWriter()

	return s
}

func (s *LogStorage) initDB() error {
	clientOptions := options.Client().ApplyURI(s.cfg.MongoURI)
	clientOptions.SetMaxPoolSize(100)
	clientOptions.SetMinPoolSize(10)
	clientOptions.SetMaxConnIdleTime(10 * time.Minute)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("connect to mongodb: %w", err)
	}
	
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping mongodb: %w", err)
	}
	
	s.client = client
	s.collection = client.Database("logs").Collection("entries")
	
	indexModels := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "service", Value: 1},
				{Key: "timestamp", Value: -1},
			},
		},
		{
			Keys: bson.D{{Key: "metadata.accountId", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "created_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(604800),
		},
	}
	
	_, err = s.collection.Indexes().CreateMany(ctx, indexModels)
	if err != nil {
		return fmt.Errorf("create indexes: %w", err)
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
		log.Printf("🚨 Queue full! Size: %d", cap(s.queue))
		return errors.New("queue full")
	}
}

func (s *LogStorage) batchWriter() {
	defer s.wg.Done()
	
	buffer := make([]LogRecord, 0, 500)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		
		log.Printf("📦 Flushing batch: %d records", len(buffer))
		
		if err := s.insertBatch(buffer); err != nil {
			log.Printf("❌ Batch insert failed: %v", err)
			message := FormatAlert("⚠️ Log Service: Batch Insert Failed", []AlertField{
				{Label: "Count", Value: fmt.Sprintf("%d", len(buffer))},
				{Label: "Error", Value: err.Error()},
			})
			s.notify(message)
		} else {
			log.Printf("✅ Batch inserted successfully: %d records", len(buffer))
		}
		
		buffer = buffer[:0]
	}

	for {
		select {
		case record := <-s.queue:
			buffer = append(buffer, record)
			if len(buffer) >= 500 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.ctx.Done():
			for len(s.queue) > 0 {
				buffer = append(buffer, <-s.queue)
			}
			flush()
			return
		}
	}
}

func (s *LogStorage) Close() error {
	s.cancel()
	s.wg.Wait()
	
	if s.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.client.Disconnect(ctx)
	}
	return nil
}

func (s *LogStorage) insertBatch(records []LogRecord) error {
	if len(records) == 0 {
		return nil
	}

	docs := make([]interface{}, len(records))
	now := time.Now()
	
	for i, record := range records {
		docs[i] = bson.M{
			"service":    record.Service,
			"level":      string(record.Level),
			"message":    record.Message,
			"metadata":   record.Metadata,
			"timestamp":  record.Timestamp,
			"created_at": now,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := s.collection.InsertMany(ctx, docs)
	if err != nil {
		return fmt.Errorf("insert many: %w", err)
	}

	return nil
}

func (s *LogStorage) ReadLogs(query LogQuery) ([]LogRecord, int, error) {
	var metadataFilter map[string]any
	if query.Metadata != "" {
		if err := json.Unmarshal([]byte(query.Metadata), &metadataFilter); err != nil {
			return nil, 0, fmt.Errorf("invalid metadata JSON: %w", err)
		}
	}

	filter := bson.M{
		"service": query.Service,
		"timestamp": bson.M{
			"$gte": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for key, value := range metadataFilter {
		filter["metadata."+key] = value
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	total, err := s.collection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("count documents: %w", err)
	}

	findOptions := options.Find().
		SetSort(bson.D{{Key: "timestamp", Value: -1}}).
		SetLimit(int64(query.Limit)).
		SetSkip(int64(query.Offset))

	cursor, err := s.collection.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, 0, fmt.Errorf("find documents: %w", err)
	}
	defer cursor.Close(ctx)

	var records []LogRecord
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		record := LogRecord{
			Service:  doc["service"].(string),
			Level:    LogLevel(doc["level"].(string)),
			Message:  doc["message"].(string),
			Metadata: make(map[string]any),
		}

		if metadata, ok := doc["metadata"].(bson.M); ok {
			for k, v := range metadata {
				record.Metadata[k] = v
			}
		}

		if timestamp, ok := doc["timestamp"].(string); ok {
			record.Timestamp = timestamp
		} else if ts, ok := doc["timestamp"].(primitive.DateTime); ok {
			record.Timestamp = ts.Time().Format(time.RFC3339Nano)
		} else {
			record.Timestamp = ""
			message := FormatAlert("🚨 Log Service: Missing Timestamp", []AlertField{
				{Label: "Type", Value: fmt.Sprintf("%T", doc["timestamp"])},
				{Label: "Value", Value: fmt.Sprintf("%v", doc["timestamp"])},
				{Label: "Service", Value: record.Service},
			})
			s.notify(message)
		}

		records = append(records, record)
	}

	if err := cursor.Err(); err != nil {
		return nil, 0, fmt.Errorf("cursor error: %w", err)
	}

	return records, int(total), nil
}

func (s *LogStorage) GetServices() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	services, err := s.collection.Distinct(ctx, "service", bson.M{})
	if err != nil {
		return nil, fmt.Errorf("distinct services: %w", err)
	}

	result := make([]string, 0, len(services))
	for _, service := range services {
		if s, ok := service.(string); ok {
			result = append(result, s)
		}
	}

	return result, nil
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
		log.Printf("⚠️  Shutdown timeout")
	}
	if s.client != nil {
		s.client.Disconnect(context.Background())
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
