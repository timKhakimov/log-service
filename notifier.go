package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type AlertField struct {
	Label string
	Value string
}

type BotNotifier struct {
	url     string
	chatIDs []string
	client  *http.Client
}

func NewBotNotifier(cfg Config) *BotNotifier {
	if cfg.BotToken == "" || len(cfg.BotChatIDs) == 0 {
		return nil
	}
	timeout := cfg.BotTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &BotNotifier{
		url:     "https://api.telegram.org/bot" + cfg.BotToken + "/sendMessage",
		chatIDs: cfg.BotChatIDs,
		client:  &http.Client{Timeout: timeout},
	}
}

func (b *BotNotifier) Notify(text string) {
	if b == nil || text == "" {
		return
	}
	go b.send(text)
}

func (b *BotNotifier) NotifySync(text string) {
	if b == nil || text == "" {
		return
	}
	b.send(text)
}

func (b *BotNotifier) send(text string) {
	chunks := splitText(text, 4096)
	for _, chatID := range b.chatIDs {
		for _, chunk := range chunks {
			payload := map[string]any{
				"chat_id":                  chatID,
				"text":                     chunk,
				"disable_web_page_preview": true,
			}
			body, err := json.Marshal(payload)
			if err != nil {
				log.Printf("bot marshal error: %v", err)
				continue
			}
			req, err := http.NewRequest(http.MethodPost, b.url, bytes.NewReader(body))
			if err != nil {
				log.Printf("bot request error: %v", err)
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := b.client.Do(req)
			if err != nil {
				log.Printf("bot send error: %v", err)
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf("bot send status %d", resp.StatusCode)
			}
		}
	}
}

func splitText(text string, size int) []string {
	if size <= 0 {
		size = 4096
	}
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{}
	}
	chunks := make([]string, 0, (len(runes)+size-1)/size)
	for i := 0; i < len(runes); i += size {
		end := i + size
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[i:end]))
	}
	return chunks
}

func FormatAlert(title string, fields []AlertField) string {
	var builder strings.Builder
	builder.WriteString("🚨 ")
	builder.WriteString(title)
	builder.WriteString("\n")
	for _, field := range fields {
		line := strings.TrimSpace(field.Value)
		if line == "" {
			continue
		}
		label := strings.TrimSpace(field.Label)
		if label != "" {
			builder.WriteString("• ")
			builder.WriteString(label)
			builder.WriteString(": ")
			builder.WriteString(line)
		} else {
			builder.WriteString("• ")
			builder.WriteString(line)
		}
		builder.WriteString("\n")
	}
	result := strings.TrimSpace(builder.String())
	if result == "" {
		return title
	}
	return result
}
