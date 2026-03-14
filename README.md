# Intraday Stock Scanner

Cloud-ready Python scanner for NSE stocks using free Yahoo Finance data. It runs a Flask health server for Render/Railway and executes the scanner in a background thread every minute during NSE market hours.

## Features

- Scans 223 NSE stocks from `stocks_nse.csv`
- Runs every 60 seconds
- Detects volume spikes, 15-minute price drops, support breaks, and top 5 intraday losers
- Sends optional Telegram alerts
- Exposes `GET /` health endpoint returning `scanner running`
- Runs only during NSE market hours: 9:15 AM to 3:30 PM IST, Monday to Friday
- Works for local Windows runs and cloud deployment on Render or Railway

## Files

- `main.py` - Flask app and scanner worker
- `config.json` - scan settings and local Telegram fallback config
- `stocks_nse.csv` - NSE symbol universe
- `requirements.txt` - Python dependencies
- `Procfile` - gunicorn startup command for cloud deployment
- `runtime.txt` - Python runtime version
- `.env.example` - environment variables template
- `.gitignore` - Python git ignore rules
- `start.ps1` - Windows startup script
- `.github/workflows/ci.yml` - GitHub Actions validation workflow

## Local Setup

```powershell
cd "C:\Users\dguru\Music\Intraday Stock Scanner"
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

Or use the startup script:

```powershell
.\start.ps1
```

Health check:

```powershell
curl http://127.0.0.1:5000/
```

Expected response:

```text
scanner running
```

## Telegram Configuration

Preferred cloud configuration uses environment variables:

```text
RUN_SCANNER=true
TELEGRAM_ENABLED=true
TELEGRAM_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
```

Local fallback is still available in `config.json`:

```json
"telegram": {
  "enabled": false,
  "bot_token": "",
  "chat_id": ""
}
```

Environment variables override `config.json` values.

`RUN_SCANNER` controls whether the background scanner starts:

- `RUN_SCANNER=true` starts the scanner and the web server
- `RUN_SCANNER=false` starts only the web server

## Deployment Notes

- Build command: `pip install -r requirements.txt`
- Start command: `python main.py`
- Health check path: `/`
- Procfile command: `python main.py`
- The scanner thread starts automatically when the Flask app loads
- Set `RUN_SCANNER=true` on the instance that should actively scan

## Render Deployment

1. Push this project to GitHub.
2. In Render, create a new Web Service from the GitHub repository.
3. Use these settings:

```text
Environment: Python
Build Command: pip install -r requirements.txt
Start Command: python main.py
Health Check Path: /
```

4. Add environment variables in Render:

```text
RUN_SCANNER=true
TELEGRAM_ENABLED=true
TELEGRAM_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
```

5. Deploy the service.

## GitHub Repository Setup

1. Create a new empty GitHub repository.
2. Initialize git locally if needed.
3. Add the remote.
4. Commit and push the project.
5. GitHub Actions will run `.github/workflows/ci.yml` on push and pull requests.

Example remote command:

```powershell
git remote add origin https://github.com/YOUR_USERNAME/intraday-stock-scanner.git
```

## Verification

- `GET /` should return `scanner running`
- `Procfile` points gunicorn at `main:app`
- `runtime.txt` pins Python `3.12.9`
- Telegram env vars are supported through `.env.example` and `main.py`
- `start.ps1` creates `.venv`, installs dependencies, and starts the app on Windows
- GitHub Actions compiles `main.py`, imports `main.app`, and verifies `GET /`

## Push To GitHub And Deploy On Render

```powershell
cd "C:\Users\dguru\Music\Intraday Stock Scanner"
git init
git add .
git commit -m "Initial cloud-ready intraday stock scanner"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/intraday-stock-scanner.git
git push -u origin main
```

Then in Render:

1. Open `https://dashboard.render.com/`
2. Create a new Web Service from the GitHub repo
3. Set build command to `pip install -r requirements.txt`
4. Set start command to `python main.py`
5. Set health check path to `/`
6. Add `TELEGRAM_ENABLED`, `TELEGRAM_TOKEN`, and `TELEGRAM_CHAT_ID`
7. Deploy

## Notes

- Yahoo Finance is free and unofficial, so temporary throttling or missing candles can happen.
- If throttling appears, reduce `batch_size` or increase `batch_pause_seconds` in `config.json`.
- The scanner suppresses duplicate alerts for the same stock and signal combination for 10 minutes by default.
- If you run both local and cloud with `RUN_SCANNER=true`, both instances will scan and both can send Telegram alerts.
- Recommended setup: keep cloud on `RUN_SCANNER=true`; use local with `RUN_SCANNER=false` unless you intentionally want a second scanner.
