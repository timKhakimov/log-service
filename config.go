package main

import (
    "errors"
    "fmt"
    "os"
    "strconv"
    "strings"
    "time"
)

type Config struct {
    BindAddr       string
    Port           int
    MongoURI       string
    FlushLines     int
    FlushInterval  time.Duration
    MaxQueue       int
    MaxRetries     int
    RetryBackoff   BackoffConfig
    BotToken       string
    BotChatIDs     []string
    BotTimeout     time.Duration
}

type BackoffConfig struct {
    Min time.Duration
    Max time.Duration
    Factor float64
}

func LoadConfig() (Config, error) {
	cfg := Config{
		BindAddr:      getEnvString("BIND_ADDR", "0.0.0.0"),
		MongoURI:      getEnvString("MONGO_URI", "mongodb://localhost:27017"),
		FlushLines:    getEnvInt("FLUSH_LINES", 500),
		FlushInterval: getEnvDuration("FLUSH_MS", 2000*time.Millisecond),
		MaxQueue:      getEnvInt("MAX_QUEUE", 100000),
		MaxRetries:    getEnvInt("MAX_RETRIES", 3),
	}
    cfg.Port = getEnvInt("PORT", 9010)
    cfg.RetryBackoff = BackoffConfig{
        Min: getEnvDuration("RETRY_BACKOFF_MIN_MS", 100*time.Millisecond),
        Max: getEnvDuration("RETRY_BACKOFF_MAX_MS", 5*time.Second),
        Factor: getEnvFloat("RETRY_BACKOFF_FACTOR", 2),
    }
    cfg.BotToken = getEnvString("CRM_BOT_TOKEN", "8406722018:AAE76vUtNg_xaCcNbAP45WImEgJUxIJsPUY")
    cfg.BotChatIDs = getEnvStringSlice("CRM_BOT_CHAT_IDS", []string{"483779758", "7938128354"})
    cfg.BotTimeout = getEnvDuration("CRM_BOT_TIMEOUT_MS", 10000*time.Millisecond)
    if cfg.Port <= 0 || cfg.Port > 65535 {
        return Config{}, errors.New("invalid port")
    }
    if cfg.FlushLines <= 0 {
        return Config{}, errors.New("FLUSH_LINES must be positive")
    }
    if cfg.FlushInterval <= 0 {
        return Config{}, errors.New("FLUSH_MS must be positive")
    }
    if cfg.MaxQueue <= 0 {
        return Config{}, errors.New("MAX_QUEUE must be positive")
    }
    if cfg.RetryBackoff.Min <= 0 || cfg.RetryBackoff.Max <= 0 {
        return Config{}, errors.New("retry backoff durations must be positive")
    }
    if cfg.RetryBackoff.Max < cfg.RetryBackoff.Min {
        return Config{}, errors.New("retry backoff max < min")
    }
    if cfg.RetryBackoff.Factor < 1 {
        return Config{}, errors.New("retry backoff factor must be >= 1")
    }
    return cfg, nil
}

func getEnvString(key, def string) string {
    if val, ok := os.LookupEnv(key); ok && val != "" {
        return val
    }
    return def
}

func getEnvInt(key string, def int) int {
    if val, ok := os.LookupEnv(key); ok && val != "" {
        parsed, err := strconv.Atoi(val)
        if err == nil {
            return parsed
        }
    }
    return def
}

func getEnvFloat(key string, def float64) float64 {
    if val, ok := os.LookupEnv(key); ok && val != "" {
        parsed, err := strconv.ParseFloat(val, 64)
        if err == nil {
            return parsed
        }
    }
    return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
    if val, ok := os.LookupEnv(key); ok && val != "" {
        parsed, err := strconv.Atoi(val)
        if err == nil {
            return time.Duration(parsed) * time.Millisecond
        }
    }
    return def
}

func getEnvStringSlice(key string, def []string) []string {
    if val, ok := os.LookupEnv(key); ok && val != "" {
        items := strings.Split(val, ",")
        result := make([]string, 0, len(items))
        for _, item := range items {
            trimmed := strings.TrimSpace(item)
            if trimmed != "" {
                result = append(result, trimmed)
            }
        }
        if len(result) > 0 {
            return result
        }
    }
    return def
}

func (c Config) Addr() string {
    return fmt.Sprintf("%s:%d", c.BindAddr, c.Port)
}

