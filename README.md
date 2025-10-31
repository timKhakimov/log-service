# Log Service

High-performance centralized logging service built with Go.

## Features
- 50k+ RPS capability
- 16 parallel workers
- NDJSON format
- Automatic file rotation by service/date
- Telegram alerts on errors
- Graceful shutdown
- PM2 integration

## Quick Start
./start.sh

## Commands
./start.sh              - Build and start
pm2 logs log-service    - View logs
pm2 restart log-service - Restart
pm2 stop log-service    - Stop
pm2 delete log-service  - Remove from PM2

## Configuration
Environment variables in ecosystem.config.js:
- PORT: HTTP port (default: 9010)
- BIND_ADDR: Bind address (default: 0.0.0.0)
- LOG_DIR: Logs directory (default: ./logs)
- FLUSH_LINES: Flush after N lines (default: 500)
- FLUSH_MS: Flush interval ms (default: 2000)
- MAX_QUEUE: Queue size (default: 100000)
- MAX_RETRIES: Write retries (default: 3)

## API
POST /log
{
  "service": "sender",
  "level": "info|warn|error|debug",
  "message": "Log message",
  "metadata": { "key": "value" }
}

Response: 200 OK
