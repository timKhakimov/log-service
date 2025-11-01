package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

type Server struct {
	cfg     Config
	storage *LogStorage
	http    *http.Server
}

func NewServer(cfg Config, storage *LogStorage) *Server {
	srv := &Server{cfg: cfg, storage: storage}
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleIndex)
	mux.HandleFunc("/log", srv.handleLog)
	mux.HandleFunc("/logs", srv.handleGetLogs)
	mux.HandleFunc("/services", srv.handleGetServices)
	srv.http = &http.Server{
		Addr:              cfg.Addr(),
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 16,
		ReadHeaderTimeout: 5 * time.Second,
		ConnState: func(conn net.Conn, state http.ConnState) {
			if state == http.StateNew {
				if tcpConn, ok := conn.(*net.TCPConn); ok {
					tcpConn.SetKeepAlive(true)
					tcpConn.SetKeepAlivePeriod(3 * time.Minute)
					tcpConn.SetNoDelay(true)
				}
			}
		},
	}
	return srv
}

func (s *Server) Start() error {
	log.Printf("🚀 Log-service started on %s", s.cfg.Addr())
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, "web/index.html")
}

func (s *Server) handleLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	
	start := time.Now()
	
	var req LogRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 10<<20))
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	
	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	record, err := req.ToRecord(time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	if err := s.storage.Insert(record); err != nil {
		log.Printf("❌ Insert failed: %v", err)
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	
	duration := time.Since(start)
	if duration > 3*time.Second {
		message := FormatAlert("⚠️  Log Service: Slow Write", []AlertField{
			{Label: "Duration", Value: duration.String()},
			{Label: "Service", Value: req.Service},
		})
		s.storage.notify(message)
	}
	
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	service := r.URL.Query().Get("service")
	if service == "" {
		writeError(w, http.StatusBadRequest, "service parameter is required")
		return
	}

	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
			if limit > 10000 {
				limit = 10000
			}
		}
	}

	offset := 0
	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	metadata := r.URL.Query().Get("metadata")

	query := LogQuery{
		Service:  service,
		Limit:    limit,
		Offset:   offset,
		Metadata: metadata,
	}

	logs, total, err := s.storage.ReadLogs(query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := LogsResponse{
		Logs:   logs,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	services, err := s.storage.GetServices()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"services": services,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"status": "error",
		"error":  message,
	})
}
