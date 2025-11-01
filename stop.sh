#!/bin/bash

echo "🛑 Stopping log-service..."
pm2 delete log-service 2>/dev/null || true

echo "🛑 Stopping MongoDB..."
docker-compose down

echo "✅ All stopped!"

