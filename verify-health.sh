#!/usr/bin/env bash
#TraderMate API 健康检查和启动回显日志

set -e

echo "=== TraderMate API Health Check ==="
echo "Timestamp: $(date)"

# Wait for container to be healthy (max 60 seconds)
echo "Waiting for container health status..."
for i in 1 2 3 4 5 6 7 8 9 10 11 12; do
    STATUS=$(docker inspect tradermate_api --format='{{.State.Health.Status}}' 2>/dev/null || echo "starting")
    if [ "$STATUS" = "healthy" ]; then
        echo "✅ Container is healthy!"
        break
    fi
    echo "  Attempt $i/12: status=$STATUS, waiting 5s..."
    sleep 5
done

# Show final health status
FINAL_STATUS=$(docker inspect tradermate_api --format='{{.State.Health.Status}}' 2>/dev/null || echo "unknown")
echo "Final health status: $FINAL_STATUS"

# Test /health endpoint
echo "Testing /health endpoint..."
if curl -s -f http://localhost:8000/health > /dev/null; then
    echo "✅ GET /health returned 200 OK"
    curl -s http://localhost:8000/health | head -20
else
    echo "❌ GET /health failed (curl exit code $?)"
fi

# Summary
if [ "$FINAL_STATUS" = "healthy" ]; then
    echo ""
    echo "🎉 TraderMate API is up and healthy!"
    echo "  - API docs: http://localhost:8000/docs"
    echo "  - API info: http://localhost:8000/api"
    echo "  - Health: http://localhost:8000/health"
else
    echo ""
    echo "⚠️  Container not healthy yet. Check logs:"
    echo "  docker logs tradermate_api"
fi
