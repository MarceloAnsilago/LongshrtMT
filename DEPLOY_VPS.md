# MT5 Bridge - Deploy VPS

## Env vars
- `MT5_BRIDGE_API_KEY`: required API key for protected endpoints.
- `MT5_BRIDGE_ALLOWED_IPS`: optional comma-separated allowlist (ex: `1.2.3.4,5.6.7.8`).
- `MT5_BRIDGE_TRUST_PROXY`: set `true` to trust `X-Forwarded-For` (default: false).
- `MT5_BRIDGE_RATE_LIMIT_PER_MIN`: requests per minute per IP (default: 60; set `0` to disable).
- `MT5_BRIDGE_EXPOSE_DOCS`: set `true` to allow public access to `/docs` and `/openapi.json`.
- `MT5_BRIDGE_CORS_ORIGINS`: optional comma-separated CORS allowlist (ex: `https://app.example.com`).

## Run (Uvicorn)
```bash
uvicorn mt5_bridge.api:app --host 0.0.0.0 --port 8001 --workers 1
```

## Quick checks
```bash
# health is public
curl http://IP_DA_VPS:8001/health

# protected endpoint (example)
curl -H "X-API-Key: $MT5_BRIDGE_API_KEY" http://IP_DA_VPS:8001/api/ping
```

## Security checklist (minimum)
- Firewall/NSG: open port 8001 only to required IPs (Fly.io egress IPs if possible).
- Use a strong, random API key.
- Keep `MT5_BRIDGE_TRUST_PROXY=false` unless you are behind a trusted proxy.

## Optional: systemd service
Create `/etc/systemd/system/mt5_bridge.service`:
```ini
[Unit]
Description=MT5 Bridge API
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/repo
EnvironmentFile=/path/to/repo/.env
ExecStart=/path/to/repo/venv/Scripts/uvicorn mt5_bridge.api:app --host 0.0.0.0 --port 8001 --workers 1
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now mt5_bridge
sudo systemctl status mt5_bridge
```
