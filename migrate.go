package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

func migrateNDJSONToSQLite(logsDir, dbPath string) error {
	db, err := sql.Open("sqlite", dbPath+"?_journal=WAL&_sync=NORMAL&_cache_size=-64000")
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

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

	services, err := os.ReadDir(logsDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("No logs directory found, nothing to migrate")
			return nil
		}
		return fmt.Errorf("read logs dir: %w", err)
	}

	totalRecords := 0
	for _, service := range services {
		if !service.IsDir() {
			continue
		}

		serviceName := service.Name()
		serviceDir := filepath.Join(logsDir, serviceName)
		pattern := filepath.Join(serviceDir, "*.ndjson")

		files, err := filepath.Glob(pattern)
		if err != nil {
			log.Printf("glob error for service %s: %v", serviceName, err)
			continue
		}

		for _, filePath := range files {
			count, err := migrateFile(db, serviceName, filePath)
			if err != nil {
				log.Printf("migrate file %s failed: %v", filePath, err)
				continue
			}
			totalRecords += count
			log.Printf("Migrated %s: %d records", filePath, count)
		}
	}

	log.Printf("Total migrated: %d records", totalRecords)
	return nil
}

func migrateFile(db *sql.DB, serviceName, filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO logs (service, level, message, metadata, timestamp, received_at) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 100*1024*1024)

	count := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record struct {
			Level     string         `json:"level"`
			Message   string         `json:"message"`
			Metadata  map[string]any `json:"metadata"`
			Timestamp string         `json:"timestamp"`
		}

		if err := json.Unmarshal(line, &record); err != nil {
			continue
		}

		timestamp, err := time.Parse(time.RFC3339Nano, record.Timestamp)
		if err != nil {
			timestamp, err = time.Parse(time.RFC3339, record.Timestamp)
			if err != nil {
				continue
			}
		}

		var metadataJSON []byte
		if len(record.Metadata) > 0 {
			metadataJSON, _ = json.Marshal(record.Metadata)
		}

		_, err = stmt.Exec(
			serviceName,
			record.Level,
			record.Message,
			metadataJSON,
			timestamp.UTC().Format(time.RFC3339Nano),
			timestamp.UTC().Format(time.RFC3339Nano),
		)
		if err != nil {
			log.Printf("insert failed: %v", err)
			continue
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return count, err
	}

	if err := tx.Commit(); err != nil {
		return count, err
	}

	return count, nil
}

func main() {
	logsDir := "./logs"
	dbPath := "./logs.db"

	if len(os.Args) > 1 {
		logsDir = os.Args[1]
	}
	if len(os.Args) > 2 {
		dbPath = os.Args[2]
	}

	fmt.Printf("Migrating from %s to %s...\n", logsDir, dbPath)

	if _, err := os.Stat(dbPath); err == nil {
		fmt.Print("Database already exists. Overwrite? (yes/no): ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(strings.TrimSpace(response)) != "yes" {
			fmt.Println("Migration cancelled")
			return
		}
		os.Remove(dbPath)
		os.Remove(dbPath + "-shm")
		os.Remove(dbPath + "-wal")
	}

	if err := migrateNDJSONToSQLite(logsDir, dbPath); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	fmt.Println("Migration completed successfully!")
}

