#!/bin/bash
set -e

echo "🐳 Starting MongoDB..."
docker-compose up -d

echo "⏳ Waiting for MongoDB to be ready..."
for i in {1..30}; do
  if docker exec $(docker-compose ps -q mongodb) mongosh --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo "✅ MongoDB is ready!"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "❌ MongoDB failed to start"
    exit 1
  fi
  sleep 1
done

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
echo "🗄️  MongoDB: docker-compose logs -f mongodb"
