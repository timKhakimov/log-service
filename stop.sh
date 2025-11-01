#!/bin/bash

echo "🛑 Stopping log-service..."
pm2 delete log-service 2>/dev/null || true

echo "🛑 Stopping MongoDB..."
docker stop log-service-mongodb 2>/dev/null || true
docker rm log-service-mongodb 2>/dev/null || true

echo "✅ All stopped!"
