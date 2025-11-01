package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	defer recoverPanic()
	cfg, err := LoadConfig()
	if err != nil {
		message := FormatAlert("Log Service: Config Error", []AlertField{{Label: "Error", Value: err.Error()}})
		notifyCritical(message)
		log.Fatalf("config error: %v", err)
	}
	notifier := NewBotNotifier(cfg)
	storage := NewLogStorage(cfg, notifier)
	srv := NewServer(cfg, storage)
	go func() {
		defer recoverPanicWithNotifier(notifier)
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			message := FormatAlert("Log Service: Start Error", []AlertField{{Label: "Error", Value: err.Error()}})
			notifyWithNotifier(notifier, message)
			log.Fatalf("server error: %v", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		message := FormatAlert("⚠️  Log Service: Shutdown Error", []AlertField{{Label: "Error", Value: err.Error()}})
		notifyWithNotifier(notifier, message)
	}
	storage.Close()
}

func notifyWithNotifier(notifier *BotNotifier, message string) {
	if message == "" {
		return
	}
	if notifier != nil {
		notifier.Notify(message)
		return
	}
	notifyCritical(message)
}

func notifyCritical(message string) {
	if message == "" {
		return
	}
	notifier := envNotifier()
	if notifier != nil {
		notifier.NotifySync(message)
	}
}

func envNotifier() *BotNotifier {
	token := os.Getenv("CRM_BOT_TOKEN")
	if token == "" {
		token = "8406722018:AAE76vUtNg_xaCcNbAP45WImEgJUxIJsPUY"
	}
	chatIDs := parseChatIDs(os.Getenv("CRM_BOT_CHAT_IDS"))
	if len(chatIDs) == 0 {
		chatIDs = []string{"483779758", "7938128354"}
	}
	timeout := 10 * time.Second
	if raw := os.Getenv("CRM_BOT_TIMEOUT_MS"); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			timeout = time.Duration(ms) * time.Millisecond
		}
	}
	cfg := Config{BotToken: token, BotChatIDs: chatIDs, BotTimeout: timeout}
	return NewBotNotifier(cfg)
}

func parseChatIDs(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func recoverPanic() {
	if r := recover(); r != nil {
		stack := string(debug.Stack())
		message := FormatAlert("🚨 Log Service: PANIC", []AlertField{
			{Label: "Error", Value: fmt.Sprintf("%v", r)},
			{Label: "Stack", Value: stack},
		})
		notifyCritical(message)
		panic(r)
	}
}

func recoverPanicWithNotifier(notifier *BotNotifier) {
	if r := recover(); r != nil {
		stack := string(debug.Stack())
		message := FormatAlert("🚨 Log Service: PANIC", []AlertField{
			{Label: "Error", Value: fmt.Sprintf("%v", r)},
			{Label: "Stack", Value: stack},
		})
		notifyWithNotifier(notifier, message)
		panic(r)
	}
}
