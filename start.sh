#!/bin/bash
set -e

echo "🔨 Building log-service..."
mkdir -p build
go build -o build/log-service .

echo "✅ Build complete!"
echo "🚀 Starting with PM2..."
pm2 delete log-service 2>/dev/null || true
pm2 start ecosystem.config.js

echo ""
pm2 status
echo ""
echo "📊 Logs: pm2 logs log-service"
