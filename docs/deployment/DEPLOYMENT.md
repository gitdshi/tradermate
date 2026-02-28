# TraderMate Production Deployment Guide

**Version**: 1.0  
**Last Updated**: 2026-02-28  
**Target**: Production deployment of TraderMate backend API and services

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Deployment Steps](#deployment-steps)
- [SSL/TLS Setup](#ssltls-setup)
- [Monitoring & Health Checks](#monitoring--health-checks)
- [Security Hardening](#security-hardening)
- [Backup Strategy](#backup-strategy)
- [Troubleshooting](#troubleshooting)

---

## Overview

TraderMate is a personal quantitative trading platform composed of:

- **FastAPI backend** (`tradermate`): Core API for strategy management, backtesting, and data sync
- **PostgreSQL/MySQL** database: User data, strategies, backtest results
- **Redis**: Job queue for async backtest and optimization tasks
- **Frontend** (`tradermate-portal`): React-based SPA
- **Worker service**: RQ workers for background processing

This guide covers production deployment using Docker Compose with a focus on security, reliability, and observability.

---

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Internet      │───▶│   Nginx (SSL)   │───▶│   Frontend      │
│ (Port 443/80)   │    │   Reverse Proxy  │    │   (5173)        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              │ API Proxy (8000)
                              ▼
                       ┌─────────────────┐
                       │   API Service   │
                       │  (FastAPI)      │
                       └─────────────────┘
                              │
                 ┌────────────┼────────────┐
                 ▼            ▼            ▼
            ┌────────┐  ┌────────┐  ┌────────┐
            │ MySQL  │  │ Redis  │  │ Worker │
            │   DB   │  │   Q    │  │  (RQ)  │
            └────────┘  └────────┘  └────────┘
```

---

## Prerequisites

- **Server**: Ubuntu 22.04 LTS (or compatible Linux) with at least 4GB RAM, 2 CPU cores, 50GB disk
- **Docker**: 20.10+ and Docker Compose 2.0+
- **Domain name**: Pointing to your server IP (e.g., `tradermate.yourdomain.com`)
- **SSL certificate**: From Let's Encrypt or commercial CA (or use self-signed for internal)
- **Firewall**: Allow ports 80, 443, and optionally 22 (SSH)

---

## Configuration

### 1. Environment Variables

Create a `.env` file in the `tradermate` project root (same level as `docker-compose.yml`):

```bash
# Application
APP_NAME=TraderMate
APP_VERSION=1.0.0
DEBUG=false
SECRET_KEY=<generate-with: openssl rand -hex 32>

# Database
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=<strong-random-password>
MYSQL_DATABASE=tushare

# VNPY Database (usually same as MySQL)
VN_DATABASE_HOST=127.0.0.1
VN_DATABASE_PORT=3306
VN_DATABASE_USER=root
VN_DATABASE_PASSWORD=<same-as-MYSQL_PASSWORD>
VN_DATABASE_DB=vnpy

# Tushare API (obtain from https://tushare.pro/)
TUSHARE_TOKEN=your-tushare-token

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# JWT Authentication
JWT_SECRET_KEY=<same-as-SECRET-KEY>
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# CORS (adjust to your domain)
CORS_ORIGINS=["https://tradermate.yourdomain.com"]

# Default Admin User (Security-critical!)
DEFAULT_ADMIN_USERNAME=admin
DEFAULT_ADMIN_PASSWORD=<strong-random-password>

# Logging
LOG_LEVEL=INFO
```

**Important Security Notes:**

- `MYSQL_PASSWORD`: Use a cryptographically random password (min 32 chars)
- `SECRET_KEY` / `JWT_SECRET_KEY`: Use 32+ random hex characters (`openssl rand -hex 32`)
- `DEFAULT_ADMIN_PASSWORD`: **Never use default value in production**; generate a strong unique password
- Store `.env` securely; never commit to version control

### 2. Nginx Reverse Proxy Configuration

Create `nginx/conf.d/tradermate.conf` (or use your existing web server):

```nginx
upstream tradermate_api {
    server api:8000;
}

upstream tradermate_worker {
    server worker:8000;
}

server {
    listen 80;
    server_name tradermate.yourdomain.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name tradermate.yourdomain.com;

    # SSL Certificates (Let's Encrypt)
    ssl_certificate /etc/letsencrypt/live/tradermate.yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/tradermate.yourdomain.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    # Security Headers
    add_header X-Frame-Options DENY;
    add_header X-Content-Type-Options nosniff;
    add_header X-XSS-Protection "1; mode=block";

    # Serve frontend SPA
    location / {
        proxy_pass http://portal:80;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }

    # API routes
    location /api/ {
        proxy_pass http://tradermate_api/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
    }

    # Health check endpoint (no auth)
    location /health {
        proxy_pass http://tradermate_api/health;
        access_log off;
    }

    # Worker endpoint (for debugging, restrict in production)
    location /worker/ {
        proxy_pass http://tradermate_worker/;
        # Add IP whitelist if needed: allow 127.0.0.1; deny all;
    }
}
```

**Nginx in Docker Compose** (add to `docker-compose.yml`):

```yaml
  nginx:
    image: nginx:alpine
    container_name: tradermate_nginx
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/conf.d:/etc/nginx/conf.d
      - ./ssl:/etc/letsencrypt  # SSL certificate volume
    depends_on:
      - api
      - portal
    networks:
      - tradermate_network
```

---

## Deployment Steps

### 1. Prepare the Server

```bash
# SSH into your server
ssh user@your-server-ip

# Install Docker & Docker Compose
sudo apt update
sudo apt install -y docker.io docker-compose

# Enable and start Docker
sudo systemctl enable docker
sudo systemctl start docker

# Add current user to docker group (logout/login required)
sudo usermod -aG docker $USER
```

### 2. Clone Repository

```bash
cd /opt
sudo git clone https://github.com/gitdshi/tradermate.git
cd tradermate
```

### 3. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your secure values
nano .env  # Or use your preferred editor
```

Generate required secrets:

```bash
# Generate random passwords
openssl rand -hex 32  # SECRET_KEY
openssl rand -hex 32  # MYSQL_PASSWORD (or use pwgen for alphanumeric)
```

### 4. Obtain SSL Certificate (Let's Encrypt)

```bash
# Install certbot
sudo apt install -y certbot python3-certbot-nginx

# Obtain certificate (requires nginx to be running first, see step 5)
sudo certbot --nginx -d tradermate.yourdomain.com
```

Alternatively, use self-signed for testing:

```bash
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout /etc/ssl/private/tradermate.key \
  -out /etc/ssl/certs/tradermate.crt
```

### 5. Deploy with Docker Compose

```bash
# Pull base images and build
docker-compose pull  # Pull pre-built images if available
docker-compose build --no-cache  # Build custom images

# Start all services
docker-compose up -d

# Check logs
docker-compose logs -f api
docker-compose logs -f mysql
docker-compose logs -f redis
```

### 6. Verify Deployment

```bash
# Check health endpoint
curl https://tradermate.yourdomain.com/health

# Should return JSON with status: "healthy"
```

### 7. Register Initial Admin User

If you set `DEFAULT_ADMIN_USERNAME` and `DEFAULT_ADMIN_PASSWORD` in `.env`, the admin user is auto-created on first API startup.

Verify by logging into the frontend with those credentials, then **immediately change the password** from the user profile page.

---

## SSL/TLS Setup

### Automated Renewal (Let's Encrypt)

```bash
# Test renewal
sudo certbot renew --dry-run

# Add to crontab (runs twice daily)
sudo crontab -e
# Add: 0 12 * * * /usr/bin/certbot renew --quiet
```

### Custom SSL with Nginx

Place your certificate and key in `ssl/` directory and mount them in the nginx service:

```yaml
volumes:
  - ./ssl/tradermate.crt:/etc/ssl/certs/tradermate.crt:ro
  - ./ssl/tradermate.key:/etc/ssl/private/tradermate.key:ro
```

Update `nginx.conf` paths accordingly.

---

## Monitoring & Health Checks

### Docker Health Checks

The API service already defines a health check in `docker-compose.yml`:

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 40s
```

Check status:

```bash
docker-compose ps
```

### Prometheus Metrics (Optional)

Add Prometheus monitoring by instrumenting FastAPI with `prometheus-fastapi-instrumentator`:

```python
# In app/api/main.py
from prometheus_fastapi_instrumentator import Instrumentator

Instrumentator().instrument(app).expose(app)
```

Then configure Prometheus to scrape `/metrics` endpoint.

### Grafana Dashboards

Recommended dashboards:

- **Node Exporter**: System metrics (CPU, memory, disk, network)
- **MySQL Exporter**: Database performance
- **Redis Exporter**: Cache hit rate, memory usage
- **cAdvisor**: Container metrics

---

## Security Hardening

### 1. Network Segmentation

Use Docker network isolation:

```yaml
networks:
  tradermate_network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

### 2. MySQL Security

- Change default `root` password (already done via `.env`)
- Create a separate read-only user for analytics (optional)
- Bind MySQL to internal network only (default in Docker)
- Enable MySQL slow query log:

```sql
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 2;
```

### 3. Redis Security

- Redis is bound to internal Docker network (no external exposure)
- Set a Redis password (`REDIS_PASSWORD` in `.env` if needed)

### 4. API Security

- Use HTTPS only (SSL termination at Nginx)
- Set `DEBUG=false` in production
- Enable rate limiting (consider `slowapi` middleware)
- Implement IP whitelisting for admin routes if needed
- Rotate `JWT_SECRET_KEY` periodically (requires all users to re-login)

### 5. File System Permissions

```bash
# Ensure only owner can read .env
chmod 600 .env

# MySQL data volume
chmod 700 mysql_data
```

### 6. Regular Security Updates

```bash
# Update base images
docker-compose pull
docker-compose up -d --force-recreate
```

Subscribe to security advisories for:
- Docker base images
- Python dependencies (定期运行 `pip-audit` 或 `safety check`)
- Node.js (for frontend build)

---

## Backup Strategy

### Database Backups

Use `mysqldump` to create daily backups:

```bash
# Create backup script
cat > /opt/tradermate/scripts/backup.sh << 'EOS'
#!/bin/bash
BACKUP_DIR="/backups/tradermate/$(date +%Y-%m-%d)"
mkdir -p $BACKUP_DIR

# Dump all databases
docker-compose exec -T mysql mysqldump -u root -p"$MYSQL_PASSWORD" \
  --all-databases --single-transaction --routines --triggers > $BACKUP_DIR/full_$(date +%Y%m%d_%H%M%S).sql

# Keep only last 30 days
find /backups/tradermate -type d -mtime +30 -exec rm -rf {} \;
EOS

chmod +x /opt/tradermate/scripts/backup.sh
```

Add to crontab:

```bash
0 2 * * * /opt/tradermate/scripts/backup.sh >> /var/log/tradermate-backup.log 2>&1
```

### Redis Persistence

Redis persists to volume `redis_data`. Ensure volume snapshots are included in your host-level backup plan.

### Upload to Remote Storage (Optional)

Sync backups to S3 or another remote location:

```bash
aws s3 sync /backups/tradermate s3://your-bucket/tradermate-backups/ --delete
```

### Restore Procedure

```bash
# List available backups
ls /backups/tradermate/

# Restore
docker-compose down
docker-compose up -d mysql  # Start only MySQL
docker-compose exec -T mysql mysql -u root -p"$MYSQL_PASSWORD" < /backups/tradermate/YYYY-MM-DD/full_*.sql
docker-compose up -d  # Start all services
```

---

## Troubleshooting

### API fails to start: database connection error

Check MySQL is running and credentials are correct:

```bash
docker-compose logs mysql
docker-compose exec mysql mysql -u root -p"$MYSQL_PASSWORD" -e "SHOW DATABASES;"
```

### Health check fails with 503

Inspect API logs:

```bash
docker-compose logs api | tail -50
```

Common causes:
- Redis not reachable (check `REDIS_HOST` and network)
- JWT_SECRET_KEY missing or too short
- Database schema not initialized (run `mysql/init/*.sql`)

### Frontend cannot reach API

Check Nginx configuration and CORS settings:

```bash
docker-compose logs nginx
docker-compose exec api curl http://localhost:8000/health
```

Verify `CORS_ORIGINS` includes your frontend domain.

### Worker not processing jobs

Check Redis connection and RQ worker logs:

```bash
docker-compose logs worker
docker-compose exec redis redis-cli ping  # Should return PONG
docker-compose exec redis redis-cli -u redis://redis:6379 rq info
```

### SSL Certificate renewal fails

Ensure nginx is running and domain resolves correctly. Certbot needs port 80 accessible:

```bash
sudo certbot renew --dry-run
sudo nginx -t  # Validate config
sudo systemctl reload nginx
```

---

## Maintenance

### Rolling Updates

Update services with zero downtime:

```bash
docker-compose pull
docker-compose up -d --remove-orphans
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f api

# Last N lines
docker-compose logs --tail=100 api
```

### Access Database CLI

```bash
docker-compose exec mysql mysql -u root -p"$MYSQL_PASSWORD" tushare
```

### Clear Redis Queue (Emergency)

```bash
docker-compose exec redis redis-cli -u redis://redis:6379 FLUSHDB
# Or specific queues:
docker-compose exec redis redis-cli -u redis://redis:6379 DEL rq:queue:backtest
```

---

## Support

For issues, feature requests, or contributions, please open an issue on GitHub:  
https://github.com/gitdshi/tradermate

---

*Last reviewed: 2026-02-28*
