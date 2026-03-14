from __future__ import annotations

import json
import math
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import schedule
import yfinance as yf
from flask import Flask


PROJECT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_DIR / "config.json"
STOCK_LIST_PATH = PROJECT_DIR / "stocks_nse.csv"
RUPEE = "\N{INDIAN RUPEE SIGN}"
IST = ZoneInfo("Asia/Kolkata")
MARKET_OPEN = dt_time(hour=9, minute=15)
MARKET_CLOSE = dt_time(hour=15, minute=30)

app = Flask(__name__)
scanner_thread: threading.Thread | None = None
scanner_lock = threading.Lock()


@dataclass
class ScannerConfig:
    refresh_interval_seconds: int
    intraday_interval: str
    intraday_period: str
    daily_period: str
    batch_size: int
    batch_pause_seconds: int
    request_timeout_seconds: int
    volume_spike_multiplier: float
    volume_average_bars: int
    price_drop_minutes: int
    price_drop_percent: float
    top_losers_count: int
    repeat_alert_after_minutes: int
    telegram_enabled: bool
    telegram_bot_token: str
    telegram_chat_id: str


class IntradayStockScanner:
    def __init__(self, config: ScannerConfig, symbols: List[str]) -> None:
        self.config = config
        self.symbols = symbols
        self.alert_cache: Dict[Tuple[str, str], datetime] = {}

    def run_once(self) -> None:
        timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z")
        if not is_market_open():
            print(f"\n[{timestamp}] NSE market closed. Skipping scan.")
            return

        print(f"\n[{timestamp}] Running scan for {len(self.symbols)} NSE stocks...")

        intraday_data = self._download_intraday_data()
        if not intraday_data:
            print("No intraday data returned from Yahoo Finance.")
            return

        previous_day_lows = self._download_previous_day_lows(list(intraday_data.keys()))
        alerts = []
        loser_rows = []

        for symbol, frame in intraday_data.items():
            cleaned = self._prepare_intraday_frame(frame)
            if cleaned.empty or len(cleaned) < 2:
                continue

            current_price = float(cleaned["Close"].iloc[-1])
            current_volume = float(cleaned["Volume"].iloc[-1])
            session_open = float(cleaned["Open"].iloc[0])
            intraday_change = ((current_price / session_open) - 1.0) * 100 if session_open else math.nan
            loser_rows.append(
                {
                    "symbol": symbol,
                    "current_price": current_price,
                    "intraday_change_percent": intraday_change,
                }
            )

            signals = []

            avg_volume = self._average_recent_volume(cleaned)
            if avg_volume and current_volume > self.config.volume_spike_multiplier * avg_volume:
                signals.append("volume spike")

            if self._has_intraday_price_drop(cleaned):
                signals.append("price drop")

            previous_day_low = previous_day_lows.get(symbol)
            if previous_day_low is not None and current_price < previous_day_low:
                signals.append("support break")

            if signals:
                alerts.append((symbol, current_price, signals, intraday_change))

        top_losers = self._top_losers(loser_rows)
        self._print_top_losers(top_losers)

        if not alerts:
            print("No active alerts.")
            return

        for symbol, price, signals, intraday_change in alerts:
            signature = " + ".join(signals)
            if not self._should_send_alert(symbol, signature):
                continue

            message = (
                "ALERT\n"
                f"Stock: {symbol}\n"
                f"Price: {RUPEE}{price:.2f}\n"
                f"Signal: {signature}\n"
                f"Intraday Change: {intraday_change:.2f}%"
            )
            print(message)
            print("-" * 40)
            self._send_telegram_alert(message)

    def _download_intraday_data(self) -> Dict[str, pd.DataFrame]:
        result: Dict[str, pd.DataFrame] = {}
        for batch in chunked(self.symbols, self.config.batch_size):
            try:
                raw = yf.download(
                    tickers=" ".join(batch),
                    period=self.config.intraday_period,
                    interval=self.config.intraday_interval,
                    auto_adjust=False,
                    progress=False,
                    group_by="ticker",
                    threads=False,
                    timeout=self.config.request_timeout_seconds,
                )
            except Exception as exc:
                print(f"Intraday download failed for batch starting with {batch[0]}: {exc}")
                time.sleep(self.config.batch_pause_seconds)
                continue

            extracted = self._extract_symbol_frames(raw, batch)
            result.update(extracted)
            time.sleep(self.config.batch_pause_seconds)
        return result

    def _download_previous_day_lows(self, symbols: List[str]) -> Dict[str, float]:
        previous_day_lows: Dict[str, float] = {}
        for batch in chunked(symbols, self.config.batch_size):
            try:
                raw = yf.download(
                    tickers=" ".join(batch),
                    period=self.config.daily_period,
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    group_by="ticker",
                    threads=False,
                    timeout=self.config.request_timeout_seconds,
                )
            except Exception as exc:
                print(f"Daily download failed for batch starting with {batch[0]}: {exc}")
                time.sleep(self.config.batch_pause_seconds)
                continue

            extracted = self._extract_symbol_frames(raw, batch)
            today = datetime.now().date()
            for symbol, frame in extracted.items():
                cleaned = frame.dropna(subset=["Low"]).copy()
                if cleaned.empty:
                    continue

                cleaned.index = pd.to_datetime(cleaned.index)
                completed_days = cleaned[cleaned.index.date < today]
                if completed_days.empty:
                    continue

                previous_day_lows[symbol] = float(completed_days["Low"].iloc[-1])
            time.sleep(self.config.batch_pause_seconds)
        return previous_day_lows

    def _extract_symbol_frames(self, raw: pd.DataFrame, symbols: List[str]) -> Dict[str, pd.DataFrame]:
        frames: Dict[str, pd.DataFrame] = {}
        if raw.empty:
            return frames

        if isinstance(raw.columns, pd.MultiIndex):
            level_zero = set(raw.columns.get_level_values(0))
            if any(symbol in level_zero for symbol in symbols):
                for symbol in symbols:
                    if symbol not in level_zero:
                        continue
                    frame = raw[symbol].copy()
                    if not frame.empty:
                        frames[symbol] = frame
                return frames

            level_one = set(raw.columns.get_level_values(1))
            if any(symbol in level_one for symbol in symbols):
                for symbol in symbols:
                    if symbol not in level_one:
                        continue
                    frame = raw.xs(symbol, axis=1, level=1).copy()
                    if not frame.empty:
                        frames[symbol] = frame
                return frames

        if len(symbols) == 1:
            frames[symbols[0]] = raw.copy()

        return frames

    def _prepare_intraday_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        cleaned = frame.dropna(subset=["Open", "Close", "Volume"]).copy()
        if cleaned.empty:
            return cleaned
        cleaned.index = pd.to_datetime(cleaned.index)
        cleaned = cleaned[~cleaned.index.duplicated(keep="last")]
        return cleaned.sort_index()

    def _average_recent_volume(self, frame: pd.DataFrame) -> float | None:
        lookback = self.config.volume_average_bars
        if len(frame) <= lookback:
            return None
        recent = frame["Volume"].iloc[-(lookback + 1):-1]
        mean_volume = float(recent.mean())
        return mean_volume if mean_volume > 0 else None

    def _has_intraday_price_drop(self, frame: pd.DataFrame) -> bool:
        bars = self.config.price_drop_minutes
        if len(frame) <= bars:
            return False
        reference_price = float(frame["Close"].iloc[-(bars + 1)])
        current_price = float(frame["Close"].iloc[-1])
        if reference_price <= 0:
            return False
        drop_percent = ((current_price / reference_price) - 1.0) * 100
        return drop_percent <= -abs(self.config.price_drop_percent)

    def _top_losers(self, rows: List[Dict[str, float]]) -> List[Dict[str, float]]:
        valid_rows = [row for row in rows if not math.isnan(row["intraday_change_percent"])]
        valid_rows.sort(key=lambda row: row["intraday_change_percent"])
        return valid_rows[: self.config.top_losers_count]

    def _print_top_losers(self, top_losers: List[Dict[str, float]]) -> None:
        if not top_losers:
            print("Top losers unavailable.")
            return

        print("Top 5 Intraday Losers")
        for index, row in enumerate(top_losers, start=1):
            print(
                f"{index}. {row['symbol']} | Price: {RUPEE}{row['current_price']:.2f} | "
                f"Change: {row['intraday_change_percent']:.2f}%"
            )
        print("-" * 40)

    def _should_send_alert(self, symbol: str, signature: str) -> bool:
        key = (symbol, signature)
        now = datetime.now()
        last_sent = self.alert_cache.get(key)
        cooldown = timedelta(minutes=self.config.repeat_alert_after_minutes)
        if last_sent and (now - last_sent) < cooldown:
            return False
        self.alert_cache[key] = now
        return True

    def _send_telegram_alert(self, message: str) -> None:
        if not self.config.telegram_enabled:
            return
        if not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            return

        url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
        payload = {"chat_id": self.config.telegram_chat_id, "text": message}
        try:
            response = requests.post(url, json=payload, timeout=self.config.request_timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"Telegram alert failed: {exc}")


def load_config(path: Path) -> ScannerConfig:
    data = json.loads(path.read_text(encoding="utf-8"))
    telegram = data.get("telegram", {})
    telegram_enabled = os.environ.get("TELEGRAM_ENABLED")
    telegram_token = os.environ.get("TELEGRAM_TOKEN", str(telegram.get("bot_token", "")))
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", str(telegram.get("chat_id", "")))
    return ScannerConfig(
        refresh_interval_seconds=int(data["refresh_interval_seconds"]),
        intraday_interval=str(data["intraday_interval"]),
        intraday_period=str(data["intraday_period"]),
        daily_period=str(data["daily_period"]),
        batch_size=int(data["batch_size"]),
        batch_pause_seconds=int(data["batch_pause_seconds"]),
        request_timeout_seconds=int(data["request_timeout_seconds"]),
        volume_spike_multiplier=float(data["volume_spike_multiplier"]),
        volume_average_bars=int(data["volume_average_bars"]),
        price_drop_minutes=int(data["price_drop_minutes"]),
        price_drop_percent=float(data["price_drop_percent"]),
        top_losers_count=int(data["top_losers_count"]),
        repeat_alert_after_minutes=int(data["repeat_alert_after_minutes"]),
        telegram_enabled=(
            telegram_enabled.lower() == "true"
            if telegram_enabled is not None
            else bool(telegram.get("enabled", False))
        ),
        telegram_bot_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
    )


def load_symbols(path: Path) -> List[str]:
    frame = pd.read_csv(path)
    symbols = (
        frame["symbol"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda series: series.ne("")]
        .drop_duplicates()
        .tolist()
    )
    if len(symbols) < 200:
        raise ValueError("The stock list must contain at least 200 NSE symbols.")
    return symbols


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def is_market_open(now: datetime | None = None) -> bool:
    current = now.astimezone(IST) if now else datetime.now(IST)
    if current.weekday() >= 5:
        return False
    current_time = current.time().replace(tzinfo=None)
    return MARKET_OPEN <= current_time <= MARKET_CLOSE


def scanner_loop() -> None:
    config = load_config(CONFIG_PATH)
    symbols = load_symbols(STOCK_LIST_PATH)
    scanner = IntradayStockScanner(config, symbols)

    scanner.run_once()
    schedule.every(config.refresh_interval_seconds).seconds.do(scanner.run_once)

    print(f"Scanner is running. Refresh interval: {config.refresh_interval_seconds} seconds")
    while True:
        schedule.run_pending()
        time.sleep(1)


def start_background_scanner() -> None:
    global scanner_thread
    with scanner_lock:
        if scanner_thread and scanner_thread.is_alive():
            return

        scanner_thread = threading.Thread(
            target=scanner_loop,
            name="intraday-scanner",
            daemon=True,
        )
        scanner_thread.start()


@app.get("/")
def healthcheck() -> str:
    return "scanner running"


if env_flag("RUN_SCANNER", True):
    start_background_scanner()
else:
    print("RUN_SCANNER is disabled. Web server will start without the background scanner.")


def main() -> None:
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
