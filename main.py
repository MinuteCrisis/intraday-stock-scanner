from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import schedule
import yfinance as yf
from flask import Flask
from flask import Response
from flask import request


PROJECT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_DIR / "config.json"
STOCK_LIST_PATH = PROJECT_DIR / "stocks_nse.csv"
LOG_DIR = PROJECT_DIR / "logs"
LOG_PATH = LOG_DIR / "scanner.log"

RUPEE = "\N{INDIAN RUPEE SIGN}"
IST = ZoneInfo("Asia/Kolkata")
MARKET_OPEN = dt_time(hour=9, minute=15)
MARKET_CLOSE = dt_time(hour=15, minute=30)
INDEX_SYMBOLS = {
    "NIFTY": "^NSEI",
    "BANKNIFTY": "^NSEBANK",
}
ALERT_SIGNAL_SCORES = {
    "VWAP breakdown": 2,
    "VWAP rejection": 2,
    "volume spike": 2,
    "support break": 2,
    "Opening Range Breakdown": 2,
    "price drop": 1,
    "market bearish": 1,
}

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("intraday_scanner")

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
    download_retries: int
    download_retry_delay_seconds: int
    volume_spike_multiplier: float
    volume_average_bars: int
    price_drop_minutes: int
    price_drop_percent: float
    top_losers_count: int
    top_gainers_count: int
    market_bearish_threshold_percent: float
    repeat_alert_after_minutes: int
    stock_mode: str
    custom_watchlist: List[str]
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
            logger.info("NSE market closed. Skipping scan.")
            print(f"\n[{timestamp}] NSE market closed. Skipping scan.")
            return

        logger.info("Scanner start for %s symbols", len(self.symbols))
        print(f"\n[{timestamp}] Running scan for {len(self.symbols)} NSE stocks...")

        scan_cache: Dict[Tuple[str, str, str], Dict[str, pd.DataFrame]] = {}
        intraday_data = self._download_intraday_data(scan_cache, self.symbols)
        if not intraday_data:
            logger.error("No intraday data returned from Yahoo Finance.")
            print("No intraday data returned from Yahoo Finance.")
            return

        previous_day_lows = self._download_previous_day_lows(scan_cache, list(intraday_data.keys()))
        index_snapshot = self._download_market_indexes(scan_cache)
        market_bearish = index_snapshot["market_bearish"]
        market_trend = index_snapshot["market_trend"]
        logger.info(
            "Market trend %s | NIFTY %.2f%% | BANKNIFTY %.2f%%",
            market_trend,
            index_snapshot["NIFTY"]["change_percent"],
            index_snapshot["BANKNIFTY"]["change_percent"],
        )

        alerts = []
        movers = []

        for symbol, frame in intraday_data.items():
            cleaned = self._prepare_intraday_frame(frame)
            if cleaned.empty or len(cleaned) < 2:
                continue

            current_price = float(cleaned["Close"].iloc[-1])
            current_volume = float(cleaned["Volume"].iloc[-1])
            session_open = float(cleaned["Open"].iloc[0])
            intraday_change = ((current_price / session_open) - 1.0) * 100 if session_open else math.nan
            vwap = self._calculate_vwap(cleaned)
            rsi = self._calculate_rsi(cleaned)
            opening_range = self._opening_range(cleaned)

            movers.append(
                {
                    "symbol": symbol,
                    "current_price": current_price,
                    "intraday_change_percent": intraday_change,
                }
            )

            signals = []
            avg_volume = self._average_recent_volume(cleaned)
            volume_spike = bool(avg_volume and current_volume > self.config.volume_spike_multiplier * avg_volume)
            if volume_spike:
                signals.append("volume spike")

            if self._has_intraday_price_drop(cleaned):
                signals.append("price drop")

            previous_day_low = previous_day_lows.get(symbol)
            if previous_day_low is not None and current_price < previous_day_low:
                signals.append("support break")

            if volume_spike and vwap is not None and current_price < vwap:
                signals.append("VWAP breakdown")

            if volume_spike and self._has_vwap_rejection(cleaned, vwap):
                signals.append("VWAP rejection")

            if opening_range is not None:
                if current_price < opening_range["low"]:
                    signals.append("Opening Range Breakdown")
                if current_price > opening_range["high"]:
                    signals.append("Opening Range Breakout")

            if rsi is not None:
                if rsi > 70:
                    signals.append("RSI overbought")
                if rsi < 30:
                    signals.append("RSI oversold")

            if market_bearish:
                signals.append("market bearish")

            score = self._score_signals(signals)
            if signals and score >= 3:
                alerts.append(
                    {
                        "symbol": symbol,
                        "price": current_price,
                        "signals": signals,
                        "score": score,
                        "intraday_change": intraday_change,
                        "vwap": vwap,
                        "rsi": rsi,
                        "market_trend": market_trend,
                    }
                )

        top_losers = self._top_movers(movers, self.config.top_losers_count, reverse=False)
        top_gainers = self._top_movers(movers, self.config.top_gainers_count, reverse=True)
        self._print_top_movers("Top Intraday Losers", top_losers)
        self._print_top_movers("Top Intraday Gainers", top_gainers)

        if not alerts:
            logger.info("No active alerts generated.")
            print("No active alerts.")
            return

        for alert in alerts:
            signature = " + ".join(alert["signals"])
            if not self._should_send_alert(alert["symbol"], signature):
                continue

            message = self._format_alert_message(alert)
            logger.info("Alert generated for %s | %s", alert["symbol"], signature)
            print(message)
            print("-" * 40)
            self._send_telegram_alert(message)

    def _format_alert_message(self, alert: Dict[str, object]) -> str:
        vwap = alert["vwap"]
        rsi = alert["rsi"]
        vwap_text = f"{RUPEE}{vwap:.2f}" if isinstance(vwap, float) else "N/A"
        rsi_text = f"{rsi:.2f}" if isinstance(rsi, float) else "N/A"
        return (
            "ALERT\n"
            f"Stock: {alert['symbol']}\n"
            f"Price: {RUPEE}{alert['price']:.2f}\n"
            f"Signals: {', '.join(alert['signals'])}\n"
            f"Score: {alert['score']}\n"
            f"VWAP: {vwap_text}\n"
            f"RSI: {rsi_text}\n"
            f"Market Trend: {alert['market_trend']}"
        )

    def _download_intraday_data(
        self,
        scan_cache: Dict[Tuple[str, str, str], Dict[str, pd.DataFrame]],
        symbols: List[str],
    ) -> Dict[str, pd.DataFrame]:
        return self._download_symbol_batches(
            scan_cache=scan_cache,
            symbols=symbols,
            period=self.config.intraday_period,
            interval=self.config.intraday_interval,
        )

    def _download_previous_day_lows(
        self,
        scan_cache: Dict[Tuple[str, str, str], Dict[str, pd.DataFrame]],
        symbols: List[str],
    ) -> Dict[str, float]:
        previous_day_lows: Dict[str, float] = {}
        daily_data = self._download_symbol_batches(
            scan_cache=scan_cache,
            symbols=symbols,
            period=self.config.daily_period,
            interval="1d",
        )
        today = datetime.now(IST).date()
        for symbol, frame in daily_data.items():
            cleaned = frame.dropna(subset=["Low"]).copy()
            if cleaned.empty:
                continue

            cleaned.index = pd.to_datetime(cleaned.index)
            completed_days = cleaned[cleaned.index.date < today]
            if completed_days.empty:
                continue

            previous_day_lows[symbol] = float(completed_days["Low"].iloc[-1])
        return previous_day_lows

    def _download_market_indexes(
        self,
        scan_cache: Dict[Tuple[str, str, str], Dict[str, pd.DataFrame]],
    ) -> Dict[str, object]:
        frames = self._download_symbol_batches(
            scan_cache=scan_cache,
            symbols=list(INDEX_SYMBOLS.values()),
            period=self.config.intraday_period,
            interval=self.config.intraday_interval,
        )
        snapshot: Dict[str, object] = {"market_bearish": False, "market_trend": "bullish"}
        index_trends: List[str] = []

        for label, symbol in INDEX_SYMBOLS.items():
            frame = self._prepare_intraday_frame(frames.get(symbol, pd.DataFrame()))
            change_percent = 0.0
            current_price = None
            vwap = None
            trend = "bullish"
            if not frame.empty and len(frame) >= 1:
                session_open = float(frame["Open"].iloc[0])
                current_price = float(frame["Close"].iloc[-1])
                if session_open > 0:
                    change_percent = ((current_price / session_open) - 1.0) * 100
                vwap = self._calculate_vwap(frame)
                if vwap is not None:
                    trend = "bearish" if current_price < vwap else "bullish"
                elif change_percent <= -abs(self.config.market_bearish_threshold_percent):
                    trend = "bearish"
            index_trends.append(trend)
            snapshot[label] = {
                "symbol": symbol,
                "change_percent": change_percent,
                "vwap": vwap,
                "current_price": current_price,
                "trend": trend,
            }

        market_bearish = any(trend == "bearish" for trend in index_trends)
        snapshot["market_bearish"] = market_bearish
        snapshot["market_trend"] = "bearish" if market_bearish else "bullish"
        return snapshot

    def _download_symbol_batches(
        self,
        scan_cache: Dict[Tuple[str, str, str], Dict[str, pd.DataFrame]],
        symbols: Sequence[str],
        period: str,
        interval: str,
    ) -> Dict[str, pd.DataFrame]:
        result: Dict[str, pd.DataFrame] = {}
        for batch in chunked(list(symbols), self.config.batch_size):
            cache_key = (period, interval, "|".join(batch))
            if cache_key in scan_cache:
                result.update(scan_cache[cache_key])
                continue

            batch_result = self._download_batch_with_retries(batch, period, interval)
            scan_cache[cache_key] = batch_result
            result.update(batch_result)
            time.sleep(self.config.batch_pause_seconds)
        return result

    def _download_batch_with_retries(
        self,
        batch: List[str],
        period: str,
        interval: str,
    ) -> Dict[str, pd.DataFrame]:
        for attempt in range(1, self.config.download_retries + 1):
            try:
                raw = yf.download(
                    tickers=" ".join(batch),
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    progress=False,
                    group_by="ticker",
                    threads=False,
                    timeout=self.config.request_timeout_seconds,
                )
                extracted = self._extract_symbol_frames(raw, batch)
                if extracted:
                    return extracted
                logger.warning(
                    "Yahoo Finance returned empty data for batch %s on attempt %s",
                    batch[0],
                    attempt,
                )
            except Exception as exc:
                logger.exception(
                    "Download failed for batch %s period=%s interval=%s attempt=%s: %s",
                    batch[0],
                    period,
                    interval,
                    attempt,
                    exc,
                )

            if attempt < self.config.download_retries:
                time.sleep(self.config.download_retry_delay_seconds)

        logger.error(
            "Exhausted retries for batch %s period=%s interval=%s",
            batch[0],
            period,
            interval,
        )
        return {}

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
        if frame.empty:
            return frame
        cleaned = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"]).copy()
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

    def _calculate_vwap(self, frame: pd.DataFrame) -> float | None:
        volume = frame["Volume"].astype(float)
        total_volume = float(volume.sum())
        if total_volume <= 0:
            return None
        typical_price = (frame["High"] + frame["Low"] + frame["Close"]) / 3
        cumulative = (typical_price * volume).sum()
        return float(cumulative / total_volume)

    def _has_vwap_rejection(self, frame: pd.DataFrame, vwap: float | None, candles: int = 5) -> bool:
        if vwap is None or len(frame) < 2:
            return False
        recent = frame.tail(candles)
        if recent.empty:
            return False
        closes = recent["Close"].astype(float)
        moved_above_vwap = bool((closes > vwap).any())
        current_below_vwap = float(closes.iloc[-1]) < vwap
        return moved_above_vwap and current_below_vwap

    def _calculate_rsi(self, frame: pd.DataFrame, period: int = 14) -> float | None:
        if len(frame) <= period:
            return None
        close = frame["Close"].astype(float)
        delta = close.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)
        avg_gain = gains.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        avg_loss = losses.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        last_gain = float(avg_gain.iloc[-1])
        last_loss = float(avg_loss.iloc[-1])
        if last_loss == 0:
            return 100.0 if last_gain > 0 else None
        rs = last_gain / last_loss
        return float(100 - (100 / (1 + rs)))

    def _opening_range(self, frame: pd.DataFrame) -> Dict[str, float] | None:
        if frame.empty:
            return None
        session_start = frame.index[0]
        range_end = session_start + timedelta(minutes=15)
        opening_range = frame[frame.index < range_end]
        if opening_range.empty:
            return None
        return {
            "high": float(opening_range["High"].max()),
            "low": float(opening_range["Low"].min()),
        }

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

    def _top_movers(
        self,
        rows: List[Dict[str, float]],
        count: int,
        reverse: bool,
    ) -> List[Dict[str, float]]:
        valid_rows = [row for row in rows if not math.isnan(row["intraday_change_percent"])]
        valid_rows.sort(key=lambda row: row["intraday_change_percent"], reverse=reverse)
        return valid_rows[:count]

    def _score_signals(self, signals: Sequence[str]) -> int:
        return sum(ALERT_SIGNAL_SCORES.get(signal, 0) for signal in signals)

    def _print_top_movers(self, title: str, rows: List[Dict[str, float]]) -> None:
        if not rows:
            print(f"{title} unavailable.")
            return

        print(title)
        for index, row in enumerate(rows, start=1):
            print(
                f"{index}. {row['symbol']} | Price: {RUPEE}{row['current_price']:.2f} | "
                f"Change: {row['intraday_change_percent']:.2f}%"
            )
        print("-" * 40)

    def _should_send_alert(self, symbol: str, signature: str) -> bool:
        key = (symbol, signature)
        now = datetime.now(IST)
        last_sent = self.alert_cache.get(key)
        cooldown = timedelta(minutes=self.config.repeat_alert_after_minutes)
        if last_sent and (now - last_sent) < cooldown:
            return False
        self.alert_cache[key] = now
        return True

    def _send_telegram_alert(self, message: str) -> None:
        if not self.config.telegram_enabled:
            logger.info("Telegram skipped because it is disabled.")
            return
        if not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            logger.warning("Telegram skipped because token or chat id is missing.")
            return

        url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
        payload = {"chat_id": self.config.telegram_chat_id, "text": message}
        try:
            response = requests.post(url, json=payload, timeout=self.config.request_timeout_seconds)
            response.raise_for_status()
            logger.info("Telegram alert sent successfully.")
        except requests.RequestException as exc:
            logger.exception("Telegram alert failed: %s", exc)


def load_config(path: Path) -> ScannerConfig:
    data = json.loads(path.read_text(encoding="utf-8"))
    telegram = data.get("telegram", {})
    telegram_enabled = os.environ.get("TELEGRAM_ENABLED")
    telegram_token = os.environ.get("TELEGRAM_TOKEN", str(telegram.get("bot_token", "")))
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", str(telegram.get("chat_id", "")))
    return ScannerConfig(
        refresh_interval_seconds=int(data.get("refresh_interval_seconds", 180)),
        intraday_interval=str(data["intraday_interval"]),
        intraday_period=str(data["intraday_period"]),
        daily_period=str(data["daily_period"]),
        batch_size=int(data["batch_size"]),
        batch_pause_seconds=int(data["batch_pause_seconds"]),
        request_timeout_seconds=int(data["request_timeout_seconds"]),
        download_retries=int(data.get("download_retries", 3)),
        download_retry_delay_seconds=int(data.get("download_retry_delay_seconds", 3)),
        volume_spike_multiplier=float(data["volume_spike_multiplier"]),
        volume_average_bars=int(data["volume_average_bars"]),
        price_drop_minutes=int(data["price_drop_minutes"]),
        price_drop_percent=float(data["price_drop_percent"]),
        top_losers_count=int(data.get("top_losers_count", 5)),
        top_gainers_count=int(data.get("top_gainers_count", 5)),
        market_bearish_threshold_percent=float(data.get("market_bearish_threshold_percent", 0.5)),
        repeat_alert_after_minutes=int(data["repeat_alert_after_minutes"]),
        stock_mode=str(data.get("stock_mode", "full")).strip().lower(),
        custom_watchlist=[
            str(symbol).strip()
            for symbol in data.get("custom_watchlist", [])
            if str(symbol).strip()
        ],
        telegram_enabled=(
            telegram_enabled.lower() == "true"
            if telegram_enabled is not None
            else bool(telegram.get("enabled", False))
        ),
        telegram_bot_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
    )


def load_symbols(config: ScannerConfig, path: Path) -> List[str]:
    if config.stock_mode == "custom":
        if not config.custom_watchlist:
            raise ValueError("custom_watchlist must contain at least one NSE symbol in custom mode.")
        return list(dict.fromkeys(config.custom_watchlist))

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
        raise ValueError("The stock list must contain at least 200 NSE symbols in full mode.")
    return symbols


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_value(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def is_market_open(now: datetime | None = None) -> bool:
    current = now.astimezone(IST) if now else datetime.now(IST)
    if current.weekday() >= 5:
        return False
    current_time = current.time().replace(tzinfo=None)
    return MARKET_OPEN <= current_time <= MARKET_CLOSE


def scanner_loop() -> None:
    config = load_config(CONFIG_PATH)
    symbols = load_symbols(config, STOCK_LIST_PATH)
    scanner = IntradayStockScanner(config, symbols)

    scanner.run_once()
    schedule.every(config.refresh_interval_seconds).seconds.do(scanner.run_once)

    logger.info("Scanner thread started. Refresh interval %s seconds", config.refresh_interval_seconds)
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


@app.get("/test-telegram")
def test_telegram() -> Response:
    expected_token = env_value("TEST_ENDPOINT_TOKEN")
    provided_token = request.args.get("token", "").strip()

    if not expected_token:
        return Response("test endpoint is disabled", status=403)

    if provided_token != expected_token:
        return Response("unauthorized", status=401)

    config = load_config(CONFIG_PATH)
    scanner = IntradayStockScanner(config, [])
    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z")
    scanner._send_telegram_alert(f"Telegram test from Intraday Stock Scanner at {timestamp}")
    return Response("telegram test sent", status=200)


if env_flag("RUN_SCANNER", True):
    start_background_scanner()
else:
    logger.info("RUN_SCANNER is disabled. Web server will start without the background scanner.")
    print("RUN_SCANNER is disabled. Web server will start without the background scanner.")


def main() -> None:
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
