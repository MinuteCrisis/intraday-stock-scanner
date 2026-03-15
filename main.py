from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import deque
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
from flask import jsonify
from flask import request


PROJECT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_DIR / "config.json"
STOCK_LIST_PATH = PROJECT_DIR / "stocks_nse.csv"
LOG_DIR = PROJECT_DIR / "logs"
LOG_PATH = LOG_DIR / "scanner.log"
STRONG_LOG_PATH = LOG_DIR / "strong_signals.log"

RUPEE = "\N{INDIAN RUPEE SIGN}"
IST = ZoneInfo("Asia/Kolkata")
MARKET_OPEN = dt_time(hour=9, minute=15)
MARKET_CLOSE = dt_time(hour=15, minute=30)
ALERT_START = dt_time(hour=9, minute=20)
ALERT_END = dt_time(hour=14, minute=45)
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
    "panic selling detected": 2,
    "panic buying detected": 2,
    "relative strength vs market": 2,
    "relative weakness vs market": 2,
    "accumulation detected": 1,
    "momentum acceleration": 1,
    "price drop": 1,
    "market bearish": 1,
}
SIGNAL_CATEGORIES = {
    "VWAP breakdown": "price_structure",
    "VWAP rejection": "price_structure",
    "Opening Range Breakdown": "price_structure",
    "Opening Range Breakout": "price_structure",
    "support break": "price_structure",
    "volume spike": "volume",
    "relative volume": "volume",
    "RSI overbought": "momentum",
    "RSI oversold": "momentum",
    "panic selling detected": "momentum",
    "panic buying detected": "momentum",
    "relative strength vs market": "momentum",
    "relative weakness vs market": "momentum",
    "accumulation detected": "momentum",
    "momentum acceleration": "momentum",
    "market bearish": "market",
    "gap up": "market",
    "gap down": "market",
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
scanner_instance: "IntradayStockScanner | None" = None
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
    gap_threshold_percent: float
    min_average_daily_volume: int
    atr_period: int
    min_atr_percent: float
    max_ranked_alerts: int
    repeat_alert_after_minutes: int
    force_market_open: bool
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
        self.pending_signals: Dict[Tuple[str, str], int] = {}
        self.daily_watchlist: List[str] = []
        self.daily_watchlist_date: datetime.date | None = None
        self.daily_symbol_alert_count: Dict[Tuple[str, datetime.date], int] = {}
        self.daily_counters_date: datetime.date | None = None
        self.total_alerts_generated = 0
        self.strong_alerts_count = 0
        self.unique_symbols_alerted: set[str] = set()
        self.daily_summary_sent_date: datetime.date | None = None
        self.recent_alerts: deque[Dict[str, object]] = deque(maxlen=100)
        self.last_scan_time: str | None = None
        self.last_health_warning_time: datetime | None = None

    def run_once(self) -> None:
        started_at = time.perf_counter()
        now = datetime.now(IST)
        self._reset_daily_state_if_needed(now)
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S %Z")
        self.last_scan_time = now.isoformat()
        if not is_market_open(self.config.force_market_open):
            self._send_daily_summary_if_needed(now)
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
        if len(intraday_data) < max(1, math.ceil(len(self.symbols) * 0.5)):
            logger.warning("Data coverage too low, skipping scan.")
            print("Data coverage too low, skipping scan.")
            return

        daily_metrics = self._download_daily_metrics(scan_cache, list(intraday_data.keys()))
        self._update_daily_watchlist(intraday_data, daily_metrics)
        symbols_to_scan = self._symbols_for_current_cycle(intraday_data.keys())
        if symbols_to_scan != list(intraday_data.keys()):
            intraday_data = {symbol: intraday_data[symbol] for symbol in symbols_to_scan if symbol in intraday_data}
            daily_metrics = {symbol: daily_metrics[symbol] for symbol in symbols_to_scan if symbol in daily_metrics}
        index_snapshot = self._download_market_indexes(scan_cache)
        market_bearish = index_snapshot["market_bearish"]
        market_trend = index_snapshot["market_trend"]
        nifty_intraday_change = float(index_snapshot["NIFTY"]["change_percent"])
        logger.info(
            "Market trend %s | NIFTY %.2f%% | BANKNIFTY %.2f%%",
            market_trend,
            index_snapshot["NIFTY"]["change_percent"],
            index_snapshot["BANKNIFTY"]["change_percent"],
        )

        if not self.config.force_market_open and not is_alert_window():
            logger.info("Outside alert window. Skipping alert generation.")
            print("Outside alert window. Skipping alert generation.")
            return

        alerts = []
        movers = []
        candidate_symbols: List[str] = []

        for symbol, frame in intraday_data.items():
            cleaned = self._prepare_intraday_frame(frame)
            if cleaned.empty or len(cleaned) < 2:
                continue

            current_price = float(cleaned["Close"].iloc[-1])
            current_volume = float(cleaned["Volume"].iloc[-1])
            cumulative_volume = float(cleaned["Volume"].sum())
            if cumulative_volume < 50_000:
                continue
            session_open = float(cleaned["Open"].iloc[0])
            intraday_change = ((current_price / session_open) - 1.0) * 100 if session_open else math.nan
            vwap = self._calculate_vwap(cleaned)
            rsi = self._calculate_rsi(cleaned)
            opening_range = self._opening_range(cleaned)
            metrics = daily_metrics.get(symbol, {})
            average_daily_volume = metrics.get("average_daily_volume")
            previous_day_low = metrics.get("previous_day_low")
            previous_close = metrics.get("previous_close")
            atr = metrics.get("atr")

            if average_daily_volume is not None and average_daily_volume < self.config.min_average_daily_volume:
                continue

            atr_percent = None
            if atr is not None and previous_close:
                atr_percent = (atr / previous_close) * 100
            if atr_percent is not None and atr_percent < self.config.min_atr_percent:
                continue

            relative_volume = None
            if average_daily_volume:
                relative_volume = cumulative_volume / average_daily_volume

            movers.append(
                {
                    "symbol": symbol,
                    "current_price": current_price,
                    "intraday_change_percent": intraday_change,
                }
            )

            signals = []
            avg_volume = self._average_recent_volume(cleaned)
            sustained_volume = self._average_last_n_volumes(cleaned, 3)
            volume_spike = bool(
                avg_volume
                and sustained_volume
                and sustained_volume > self.config.volume_spike_multiplier * avg_volume
            )
            if volume_spike:
                signals.append("volume spike")

            if self._has_intraday_price_drop(cleaned):
                signals.append("price drop")

            if previous_day_low is not None and current_price < previous_day_low:
                signals.append("support break")

            gap_percent = self._gap_percent(session_open, previous_close)
            if gap_percent is not None:
                if gap_percent <= -abs(self.config.gap_threshold_percent):
                    signals.append("gap down")
                if gap_percent >= abs(self.config.gap_threshold_percent):
                    signals.append("gap up")

            if volume_spike and vwap is not None and current_price < vwap:
                signals.append("VWAP breakdown")

            if volume_spike and self._has_vwap_rejection(cleaned, vwap):
                signals.append("VWAP rejection")

            if opening_range is not None:
                if current_price < opening_range["low"]:
                    signals.append("Opening Range Breakdown")
                if current_price > opening_range["high"]:
                    signals.append("Opening Range Breakout")

            if rsi is not None and volume_spike:
                if rsi > 70:
                    signals.append("RSI overbought")
                if rsi < 30:
                    signals.append("RSI oversold")

            if relative_volume is not None:
                if intraday_change <= -2.5 and relative_volume >= 3:
                    signals.append("panic selling detected")
                if intraday_change >= 2.5 and relative_volume >= 3:
                    signals.append("panic buying detected")

            relative_strength_value = intraday_change - nifty_intraday_change
            if relative_strength_value >= 2:
                signals.append("relative strength vs market")
            if relative_strength_value <= -2:
                signals.append("relative weakness vs market")

            if self._has_accumulation_pattern(cleaned, current_price):
                signals.append("accumulation detected")

            if self._has_momentum_acceleration(cleaned):
                signals.append("momentum acceleration")

            if market_bearish:
                signals.append("market bearish")

            score = self._score_signals(signals)
            categories = {SIGNAL_CATEGORIES[signal] for signal in signals if signal in SIGNAL_CATEGORIES}
            suggested_bias = self._suggested_bias(signals, market_trend)
            if signals and score >= 3 and len(categories) >= 2:
                candidate_symbols.append(symbol)
                range_potential = self._range_potential(intraday_change, atr_percent)
                alerts.append(
                    {
                        "symbol": symbol,
                        "price": current_price,
                        "signals": signals,
                        "score": score,
                        "intraday_change": intraday_change,
                        "vwap": vwap,
                        "rsi": rsi,
                        "gap_percent": gap_percent,
                        "relative_volume": relative_volume,
                        "market_trend": market_trend,
                        "suggested_bias": suggested_bias,
                        "confluence_strength": self._confluence_strength(signals),
                        "range_potential": range_potential,
                        "priority": self._priority_score(
                            score,
                            relative_volume,
                            gap_percent,
                            market_trend,
                            suggested_bias,
                        ),
                    }
                )

        top_losers = self._top_movers(movers, self.config.top_losers_count, reverse=False)
        top_gainers = self._top_movers(movers, self.config.top_gainers_count, reverse=True)
        self._print_top_movers("Top Intraday Losers", top_losers)
        self._print_top_movers("Top Intraday Gainers", top_gainers)

        higher_timeframe_data = self._download_higher_timeframe_data(scan_cache, candidate_symbols)
        alerts = [
            {
                **alert,
                "higher_timeframe_trend": self._higher_timeframe_trend(
                    higher_timeframe_data.get(str(alert["symbol"]), pd.DataFrame())
                ),
            }
            for alert in alerts
        ]
        alerts = [
            alert
            for alert in alerts
            if self._trend_matches_bias(str(alert["higher_timeframe_trend"]), str(alert["suggested_bias"]))
        ]
        alerts = [
            alert
            for alert in alerts
            if self._market_allows_bias(str(alert["market_trend"]), str(alert["suggested_bias"]))
        ]

        confirmed_alerts = self._confirm_alerts(alerts)
        if not confirmed_alerts:
            logger.info("No active alerts generated.")
            print("No active alerts.")
            return

        confirmed_alerts.sort(key=lambda alert: (alert["priority"], abs(alert["intraday_change"])), reverse=True)
        ranked_alerts = confirmed_alerts[: self.config.max_ranked_alerts]

        for alert in ranked_alerts:
            signature = " + ".join(alert["signals"])
            if not self._should_send_alert(alert["symbol"], signature):
                continue
            if abs(float(alert["intraday_change"])) < 0.5:
                continue
            if self._symbol_alert_limit_reached(str(alert["symbol"]), now.date()):
                continue

            message = self._format_alert_message(alert)
            logger.info(
                "Alert generated for %s | score=%s priority=%.2f signals=%s",
                alert["symbol"],
                alert["score"],
                alert["priority"],
                signature,
            )
            print(message)
            print("-" * 40)
            self._send_telegram_alert(message)
            self._record_sent_alert(alert, now)
        elapsed = time.perf_counter() - started_at
        logger.info("Scan completed in %.1f seconds.", elapsed)

    def _format_alert_message(self, alert: Dict[str, object]) -> str:
        timestamp_text = datetime.now(IST).strftime("%H:%M IST")
        vwap = alert["vwap"]
        rsi = alert["rsi"]
        gap_percent = alert["gap_percent"]
        display_symbol = self._display_symbol(str(alert["symbol"]))
        vwap_text = f"{RUPEE}{vwap:.2f}" if isinstance(vwap, float) else "N/A"
        rsi_text = f"{rsi:.2f}" if isinstance(rsi, float) else "N/A"
        gap_text = f"{gap_percent:.2f}%" if isinstance(gap_percent, float) else "N/A"
        confidence = self._confidence_level(int(alert["score"]), str(alert["confluence_strength"]))
        tv_symbol = display_symbol
        friendly_signals = "\n".join(
            f"- {self._friendly_signal_text(str(signal), rsi_text, gap_text)}"
            for signal in alert["signals"]
        )
        return (
            "🚨 TRADE ALERT\n\n"
            f"Time: {timestamp_text}\n"
            f"Stock: {display_symbol}\n"
            f"Price: {RUPEE}{alert['price']:.2f}\n\n"
            f"Possible Action: {alert['suggested_bias']}\n\n"
            f"Why this signal appeared:\n{friendly_signals}\n\n"
            f"Signal Strength: {alert['score']}\n\n"
            f"Priority Score: {alert['priority']:.2f}\n\n"
            f"Confluence: {alert['confluence_strength']}\n\n"
            f"Confidence: {confidence}\n\n"
            f"Range Potential: {alert['range_potential']}\n\n"
            f"Market Trend: {alert['market_trend']}\n"
            f"Higher Timeframe: {alert['higher_timeframe_trend']}\n\n"
            f"Chart:\nhttps://www.tradingview.com/chart/?symbol=NSE:{tv_symbol}"
        )

    def _download_intraday_data(
        self,
        scan_cache: Dict[Tuple[str, str, str], Dict[str, pd.DataFrame]],
        symbols: List[str],
    ) -> Dict[str, pd.DataFrame]:
        cache_key = (self.config.intraday_period, self.config.intraday_interval, "|".join(symbols))
        if cache_key in scan_cache:
            return scan_cache[cache_key]

        try:
            raw = yf.download(
                tickers=" ".join(symbols),
                period=self.config.intraday_period,
                interval=self.config.intraday_interval,
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=False,
                timeout=self.config.request_timeout_seconds,
            )
            extracted = self._extract_symbol_frames(raw, symbols)
            if extracted:
                scan_cache[cache_key] = extracted
                return extracted
            logger.warning("Single-call intraday download returned empty data. Falling back to batch download.")
        except Exception as exc:
            logger.exception("Single-call intraday download failed. Falling back to batch download: %s", exc)

        return self._download_symbol_batches(
            scan_cache=scan_cache,
            symbols=symbols,
            period=self.config.intraday_period,
            interval=self.config.intraday_interval,
        )

    def _download_higher_timeframe_data(
        self,
        scan_cache: Dict[Tuple[str, str, str], Dict[str, pd.DataFrame]],
        symbols: List[str],
    ) -> Dict[str, pd.DataFrame]:
        return self._download_symbol_batches(
            scan_cache=scan_cache,
            symbols=symbols,
            period=self.config.intraday_period,
            interval="15m",
        )

    def _update_daily_watchlist(
        self,
        intraday_data: Dict[str, pd.DataFrame],
        daily_metrics: Dict[str, Dict[str, float]],
    ) -> None:
        now = datetime.now(IST)
        today = now.date()
        if self.daily_watchlist_date != today:
            self.daily_watchlist = []
            self.daily_watchlist_date = today

        if now.time() >= dt_time(hour=9, minute=30) and self.daily_watchlist:
            return

        candidates = []
        for symbol, frame in intraday_data.items():
            cleaned = self._prepare_intraday_frame(frame)
            early_metrics = self._early_activity_metrics(cleaned, daily_metrics.get(symbol, {}))
            if early_metrics is None:
                continue
            candidates.append({"symbol": symbol, **early_metrics})

        if not candidates:
            return

        ranked = sorted(
            candidates,
            key=lambda row: (abs(row["gap_percent"]), row["early_volume"], abs(row["early_price_change"])),
            reverse=True,
        )
        new_watchlist = [row["symbol"] for row in ranked[:10]]
        if new_watchlist != self.daily_watchlist:
            self.daily_watchlist = new_watchlist
            logger.info("Daily watchlist for %s: %s", today.isoformat(), ", ".join(self.daily_watchlist))

    def _symbols_for_current_cycle(self, available_symbols: Iterable[str]) -> List[str]:
        now = datetime.now(IST)
        available_list = list(available_symbols)
        if now.time() >= dt_time(hour=9, minute=30) and self.daily_watchlist:
            watchlist = [symbol for symbol in self.daily_watchlist if symbol in available_list]
            if watchlist:
                return watchlist
        return available_list

    def _download_daily_metrics(
        self,
        scan_cache: Dict[Tuple[str, str, str], Dict[str, pd.DataFrame]],
        symbols: List[str],
    ) -> Dict[str, Dict[str, float]]:
        metrics_by_symbol: Dict[str, Dict[str, float]] = {}
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
            if len(completed_days) < 2:
                continue

            average_daily_volume = float(completed_days["Volume"].tail(20).mean())
            previous_close = float(completed_days["Close"].iloc[-1])
            previous_day_low = float(completed_days["Low"].iloc[-1])
            atr = self._calculate_atr(completed_days, self.config.atr_period)
            metrics_by_symbol[symbol] = {
                "average_daily_volume": average_daily_volume,
                "previous_close": previous_close,
                "previous_day_low": previous_day_low,
                "atr": atr if atr is not None else 0.0,
            }
        return metrics_by_symbol

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

    def _average_last_n_volumes(self, frame: pd.DataFrame, candles: int) -> float | None:
        if len(frame) < candles:
            return None
        recent = frame["Volume"].iloc[-candles:]
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

    def _higher_timeframe_trend(self, frame: pd.DataFrame) -> str:
        cleaned = self._prepare_intraday_frame(frame)
        if cleaned.empty:
            return "bearish"
        current_price = float(cleaned["Close"].iloc[-1])
        vwap = self._calculate_vwap(cleaned)
        if vwap is None:
            return "bearish"
        return "bullish" if current_price > vwap else "bearish"

    def _has_vwap_rejection(self, frame: pd.DataFrame, vwap: float | None, candles: int = 5) -> bool:
        if vwap is None or len(frame) < 2:
            return False
        recent = frame.tail(candles)
        if recent.empty:
            return False
        closes = recent["Close"].astype(float)
        moved_above_vwap = int((closes > vwap).sum()) >= 2
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
        session_date = frame.index[0].astimezone(IST).date() if frame.index[0].tzinfo else frame.index[0].date()
        range_start = pd.Timestamp(datetime.combine(session_date, MARKET_OPEN), tz=IST)
        range_end = pd.Timestamp(datetime.combine(session_date, dt_time(hour=9, minute=30)), tz=IST)
        if frame.index.tz is not None:
            range_start = range_start.tz_convert(frame.index.tz)
            range_end = range_end.tz_convert(frame.index.tz)
        opening_range = frame[(frame.index >= range_start) & (frame.index < range_end)]
        if opening_range.empty:
            return None
        return {
            "high": float(opening_range["High"].max()),
            "low": float(opening_range["Low"].min()),
        }

    def _calculate_atr(self, frame: pd.DataFrame, period: int) -> float | None:
        if len(frame) < period + 1:
            return None
        high = frame["High"].astype(float)
        low = frame["Low"].astype(float)
        close = frame["Close"].astype(float)
        previous_close = close.shift(1)
        true_range = pd.concat(
            [
                high - low,
                (high - previous_close).abs(),
                (low - previous_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr_series = true_range.rolling(period).mean()
        last_atr = atr_series.iloc[-1]
        return float(last_atr) if pd.notna(last_atr) else None

    def _gap_percent(self, session_open: float, previous_close: float | None) -> float | None:
        if previous_close is None or previous_close <= 0:
            return None
        return ((session_open / previous_close) - 1.0) * 100

    def _has_accumulation_pattern(self, frame: pd.DataFrame, current_price: float) -> bool:
        if len(frame) < 10 or current_price <= 0:
            return False
        recent = frame.tail(10)
        max_high = float(recent["High"].max())
        min_low = float(recent["Low"].min())
        price_range_percent = ((max_high - min_low) / current_price) * 100
        first_half_volume = float(recent["Volume"].iloc[:5].mean())
        second_half_volume = float(recent["Volume"].iloc[5:].mean())
        return price_range_percent <= 0.8 and second_half_volume > first_half_volume

    def _has_momentum_acceleration(self, frame: pd.DataFrame) -> bool:
        if len(frame) < 5:
            return False
        close = frame["Close"].astype(float)
        recent_move = abs(float(close.iloc[-1]) - float(close.iloc[-3]))
        previous_move = abs(float(close.iloc[-3]) - float(close.iloc[-5]))
        return recent_move > 2 * previous_move

    def _early_activity_metrics(
        self,
        frame: pd.DataFrame,
        daily_metrics: Dict[str, float],
    ) -> Dict[str, float] | None:
        if frame.empty:
            return None
        opening_range = self._opening_range(frame)
        if opening_range is None:
            return None
        early_frame = self._slice_opening_window(frame)
        if early_frame.empty:
            return None
        previous_close = daily_metrics.get("previous_close")
        session_open = float(frame["Open"].iloc[0])
        early_close = float(early_frame["Close"].iloc[-1])
        gap_percent = self._gap_percent(session_open, previous_close)
        early_price_change = ((early_close / session_open) - 1.0) * 100 if session_open > 0 else 0.0
        return {
            "gap_percent": gap_percent if gap_percent is not None else 0.0,
            "early_volume": float(early_frame["Volume"].sum()),
            "early_price_change": early_price_change,
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

    def _confluence_strength(self, signals: Sequence[str]) -> str:
        categories = {SIGNAL_CATEGORIES[signal] for signal in signals if signal in SIGNAL_CATEGORIES}
        if len(categories) >= 3:
            return "Strong"
        if len(categories) == 2:
            return "Moderate"
        return "Weak"

    def _priority_score(
        self,
        score: int,
        relative_volume: float | None,
        gap_percent: float | None,
        market_trend: str,
        suggested_bias: str,
    ) -> float:
        priority = float(score)
        if relative_volume is not None:
            priority += relative_volume
        if gap_percent is not None:
            priority += abs(gap_percent) / 2
        if market_trend == "bearish" and suggested_bias == "SHORT":
            priority += 0.5
        if market_trend == "bullish" and suggested_bias == "LONG":
            priority += 0.5
        return priority

    def _confidence_level(self, score: int, confluence_strength: str) -> str:
        if score >= 4 and confluence_strength == "Strong":
            return "High"
        if score >= 3 and confluence_strength in {"Strong", "Moderate"}:
            return "Medium"
        return "Low"

    def _range_potential(self, intraday_change: float, atr_percent: float | None) -> str:
        if atr_percent is None or atr_percent <= 0:
            return "Unknown"
        rep = abs(intraday_change) / atr_percent
        if rep < 0.7:
            return "High"
        if rep <= 1:
            return "Medium"
        return "Low"

    def _friendly_signal_text(self, signal: str, rsi_text: str, gap_text: str) -> str:
        descriptions = {
            "volume spike": "trading volume is much higher than normal",
            "price drop": "price fell quickly in the last few minutes",
            "support break": "price dropped below the previous day's low",
            "VWAP breakdown": "price slipped below the session average price",
            "VWAP rejection": "price tried to move above the average price and failed",
            "Opening Range Breakdown": "price fell below the first 15-minute range",
            "Opening Range Breakout": "price moved above the first 15-minute range",
            "RSI overbought": f"momentum looks overheated (RSI {rsi_text})",
            "RSI oversold": f"momentum looks weak or washed out (RSI {rsi_text})",
            "panic selling detected": "price is dropping hard with very heavy trading activity",
            "panic buying detected": "price is rising fast with very heavy trading activity",
            "relative strength vs market": "this stock is outperforming the NIFTY today",
            "relative weakness vs market": "this stock is underperforming the NIFTY today",
            "accumulation detected": "price is staying tight while volume is building up",
            "momentum acceleration": "price movement is speeding up quickly over the last few candles",
            "market bearish": "the broader market is trading weak",
            "gap down": f"the stock opened sharply lower today ({gap_text})",
            "gap up": f"the stock opened sharply higher today ({gap_text})",
        }
        return descriptions.get(signal, signal)

    def _confirm_alerts(self, alerts: List[Dict[str, object]]) -> List[Dict[str, object]]:
        active_keys = set()
        confirmed: List[Dict[str, object]] = []

        for alert in alerts:
            signature = " + ".join(alert["signals"])
            key = (str(alert["symbol"]), signature)
            active_keys.add(key)
            count = self.pending_signals.get(key, 0) + 1
            self.pending_signals[key] = count
            if count >= 2:
                confirmed.append(alert)

        stale_keys = [key for key in self.pending_signals if key not in active_keys]
        for key in stale_keys:
            del self.pending_signals[key]

        return confirmed

    def _suggested_bias(self, signals: Sequence[str], market_trend: str) -> str:
        short_signals = {
            "VWAP breakdown",
            "VWAP rejection",
            "support break",
            "Opening Range Breakdown",
            "price drop",
            "market bearish",
            "gap down",
        }
        long_signals = {
            "Opening Range Breakout",
            "RSI oversold",
            "gap up",
        }
        short_score = sum(1 for signal in signals if signal in short_signals)
        long_score = sum(1 for signal in signals if signal in long_signals)
        if market_trend == "bearish":
            short_score += 1
        if short_score >= long_score:
            return "SHORT"
        return "LONG"

    def _slice_opening_window(self, frame: pd.DataFrame) -> pd.DataFrame:
        session_date = frame.index[0].astimezone(IST).date() if frame.index[0].tzinfo else frame.index[0].date()
        range_start = pd.Timestamp(datetime.combine(session_date, MARKET_OPEN), tz=IST)
        range_end = pd.Timestamp(datetime.combine(session_date, dt_time(hour=9, minute=30)), tz=IST)
        if frame.index.tz is not None:
            range_start = range_start.tz_convert(frame.index.tz)
            range_end = range_end.tz_convert(frame.index.tz)
        return frame[(frame.index >= range_start) & (frame.index < range_end)]

    def _trend_matches_bias(self, trend: str, bias: str) -> bool:
        if bias == "LONG":
            return trend == "bullish"
        if bias == "SHORT":
            return trend == "bearish"
        return False

    def _market_allows_bias(self, market_trend: str, bias: str) -> bool:
        if market_trend == "bullish":
            return bias != "SHORT"
        if market_trend == "bearish":
            return bias != "LONG"
        return True

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
        delays = [1, 2, 4]
        for attempt in range(1, 5):
            try:
                response = requests.post(url, json=payload, timeout=self.config.request_timeout_seconds)
                response.raise_for_status()
                logger.info("Telegram alert sent successfully.")
                return
            except requests.RequestException as exc:
                logger.exception("Telegram alert failed on attempt %s: %s", attempt, exc)
                if attempt <= len(delays):
                    time.sleep(delays[attempt - 1])

    def _symbol_alert_limit_reached(self, symbol: str, today: datetime.date) -> bool:
        return self.daily_symbol_alert_count.get((symbol, today), 0) >= 3

    def _record_sent_alert(self, alert: Dict[str, object], now: datetime) -> None:
        symbol = str(alert["symbol"])
        today = now.date()
        key = (symbol, today)
        self.daily_symbol_alert_count[key] = self.daily_symbol_alert_count.get(key, 0) + 1
        self.total_alerts_generated += 1
        self.unique_symbols_alerted.add(symbol)
        self.recent_alerts.appendleft(
            {
                "time": now.strftime("%H:%M IST"),
                "symbol": self._display_symbol(symbol),
                "price": round(float(alert["price"]), 2),
                "action": str(alert["suggested_bias"]),
                "score": int(alert["score"]),
                "priority": round(float(alert["priority"]), 2),
                "confluence": str(alert["confluence_strength"]),
                "confidence": self._confidence_level(int(alert["score"]), str(alert["confluence_strength"])),
                "range_potential": str(alert["range_potential"]),
                "signals": " + ".join(str(signal) for signal in alert["signals"]),
            }
        )
        if int(alert["score"]) >= 4 and str(alert["confluence_strength"]) == "Strong":
            self.strong_alerts_count += 1
            self._log_strong_signal(alert, now)

    def _log_strong_signal(self, alert: Dict[str, object], now: datetime) -> None:
        with STRONG_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(
                f"{now.strftime('%Y-%m-%d %H:%M:%S %Z')} | "
                f"{alert['symbol']} | "
                f"{' + '.join(str(signal) for signal in alert['signals'])} | "
                f"score={alert['score']} | priority={alert['priority']:.2f}\n"
            )

    def _reset_daily_state_if_needed(self, now: datetime) -> None:
        today = now.date()
        if self.daily_counters_date == today:
            return
        self.daily_counters_date = today
        self.daily_symbol_alert_count = {}
        self.total_alerts_generated = 0
        self.strong_alerts_count = 0
        self.unique_symbols_alerted = set()
        self.daily_summary_sent_date = None

    def _send_daily_summary_if_needed(self, now: datetime) -> None:
        if self.config.force_market_open:
            return
        if now.weekday() >= 5 or now.time() < MARKET_CLOSE:
            return
        if self.daily_summary_sent_date == now.date():
            return
        summary = (
            "Daily Scanner Summary\n\n"
            f"Total Alerts: {self.total_alerts_generated}\n"
            f"Strong Alerts: {self.strong_alerts_count}\n"
            f"Unique Symbols: {len(self.unique_symbols_alerted)}\n"
            f"Strong Signal Rate: {self._strong_signal_rate():.2f}%"
        )
        self._send_telegram_alert(summary)
        self.daily_summary_sent_date = now.date()

    def _strong_signal_rate(self) -> float:
        if self.total_alerts_generated == 0:
            return 0.0
        return (self.strong_alerts_count / self.total_alerts_generated) * 100

    def _display_symbol(self, symbol: str) -> str:
        return symbol.removesuffix(".NS")

    def _check_scanner_health(self) -> None:
        if not self.last_scan_time:
            return
        now = datetime.now(IST)
        last_scan = datetime.fromisoformat(self.last_scan_time).astimezone(IST)
        if (now - last_scan) <= timedelta(minutes=10):
            return
        if self.last_health_warning_time and (now - self.last_health_warning_time) < timedelta(minutes=5):
            return
        message = (
            "Scanner Health Warning\n\n"
            f"Last successful scan: {last_scan.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
            "The scanner may have stopped or stalled."
        )
        self._send_telegram_alert(message)
        self.last_health_warning_time = now


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
        gap_threshold_percent=float(data.get("gap_threshold_percent", 2.0)),
        min_average_daily_volume=int(data.get("min_average_daily_volume", 1_000_000)),
        atr_period=int(data.get("atr_period", 14)),
        min_atr_percent=float(data.get("min_atr_percent", 1.0)),
        max_ranked_alerts=int(data.get("max_ranked_alerts", 5)),
        repeat_alert_after_minutes=int(data["repeat_alert_after_minutes"]),
        force_market_open=bool(data.get("force_market_open", False)),
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


def is_market_open(force_market_open: bool = False, now: datetime | None = None) -> bool:
    if force_market_open:
        return True
    current = now.astimezone(IST) if now else datetime.now(IST)
    if current.weekday() >= 5:
        return False
    current_time = current.time().replace(tzinfo=None)
    return MARKET_OPEN <= current_time <= MARKET_CLOSE


def is_alert_window(now: datetime | None = None) -> bool:
    current = now.astimezone(IST) if now else datetime.now(IST)
    if current.weekday() >= 5:
        return False
    current_time = current.time().replace(tzinfo=None)
    return ALERT_START <= current_time <= ALERT_END


def scanner_loop() -> None:
    global scanner_instance
    config = load_config(CONFIG_PATH)
    symbols = load_symbols(config, STOCK_LIST_PATH)
    scanner = IntradayStockScanner(config, symbols)
    scanner_instance = scanner

    scanner.run_once()
    schedule.every(config.refresh_interval_seconds).seconds.do(scanner.run_once)
    schedule.every(5).minutes.do(scanner._check_scanner_health)

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


@app.get("/dashboard")
def dashboard() -> Response:
    scanner = scanner_instance
    alerts = list(scanner.recent_alerts)[:20] if scanner else []
    top_alerts = sorted(alerts, key=lambda alert: float(alert["priority"]), reverse=True)[:5]
    last_scan = "N/A"
    if scanner and scanner.last_scan_time:
        last_scan = datetime.fromisoformat(scanner.last_scan_time).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    watchlist = ", ".join(scanner._display_symbol(symbol) for symbol in scanner.daily_watchlist) if scanner and scanner.daily_watchlist else "N/A"
    scanner_running = bool(scanner_thread and scanner_thread.is_alive())
    total_alerts_today = scanner.total_alerts_generated if scanner else 0
    watchlist_size = len(scanner.daily_watchlist) if scanner else 0
    top_rows = "".join(
        (
            "<tr>"
            f"<td>{index}</td>"
            f"<td>{alert['symbol']}</td>"
            f"<td>{alert['action']}</td>"
            f"<td>{alert['score']}</td>"
            f"<td>{alert['priority']:.2f}</td>"
            f"<td>{alert['confluence']}</td>"
            f"<td>{alert['confidence']}</td>"
            "</tr>"
        )
        for index, alert in enumerate(top_alerts, start=1)
    )
    rows = "".join(
        (
            "<tr>"
            f"<td>{alert['time']}</td>"
            f"<td>{alert['symbol']}</td>"
            f"<td>{RUPEE}{alert['price']:.2f}</td>"
            f"<td>{alert['action']}</td>"
            f"<td>{alert['score']}</td>"
            f"<td>{alert['priority']:.2f}</td>"
            f"<td>{alert['confluence']}</td>"
            f"<td>{alert['confidence']}</td>"
            f"<td>{alert['range_potential']}</td>"
            f"<td>{alert['signals']}</td>"
            "</tr>"
        )
        for alert in alerts
    )
    html = (
        "<html><head><title>Scanner Dashboard</title>"
        "<meta http-equiv='refresh' content='30'>"
        "<style>"
        "body{font-family:Arial,sans-serif;background:#f5f7fb;margin:0;padding:24px;color:#1f2937;}"
        ".container{max-width:1200px;margin:0 auto;}"
        "h1{margin-bottom:20px;text-align:center;}"
        ".section-title{font-size:20px;margin:0 0 12px 0;}"
        ".card{background:#fff;border:1px solid #dbe3ef;border-radius:12px;padding:18px 20px;margin-bottom:18px;"
        "box-shadow:0 4px 14px rgba(15,23,42,0.05);}"
        ".stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;}"
        ".stat{background:#f8fafc;border-radius:10px;padding:12px 14px;}"
        ".label{font-size:12px;text-transform:uppercase;color:#64748b;letter-spacing:.04em;}"
        ".value{font-size:18px;font-weight:700;margin-top:4px;}"
        ".watchlist{line-height:1.7;}"
        "table{border-collapse:collapse;width:100%;margin-top:8px;}"
        "th,td{border:1px solid #dbe3ef;padding:10px 12px;text-align:left;vertical-align:top;}"
        "th{background:#eef4fb;}"
        "</style>"
        "</head><body><div class='container'><h1>Intraday Stock Scanner Dashboard</h1>"
        "<div class='card'><div class='section-title'>Scanner Status</div>"
        "<div class='stats'>"
        f"<div class='stat'><div class='label'>Scanner Running</div><div class='value'>{scanner_running}</div></div>"
        f"<div class='stat'><div class='label'>Last Scan Time</div><div class='value'>{last_scan}</div></div>"
        f"<div class='stat'><div class='label'>Total Alerts Today</div><div class='value'>{total_alerts_today}</div></div>"
        f"<div class='stat'><div class='label'>Watchlist Size</div><div class='value'>{watchlist_size}</div></div>"
        "</div></div>"
        "<div class='card'><div class='section-title'>Today's Watchlist</div>"
        f"<div class='watchlist'>{watchlist}</div></div>"
        "<div class='card'><div class='section-title'>Top 5 Trade Opportunities</div>"
        "<table><thead><tr><th>Rank</th><th>Symbol</th><th>Action</th><th>Score</th><th>Priority</th>"
        "<th>Confluence</th><th>Confidence</th></tr></thead>"
        f"<tbody>{top_rows}</tbody></table></div>"
        "<div class='card'><div class='section-title'>Recent Alerts</div>"
        "<table><thead><tr><th>Time</th><th>Symbol</th><th>Price</th><th>Action</th><th>Score</th>"
        "<th>Priority</th><th>Confluence</th><th>Confidence</th><th>Range Potential</th><th>Signals</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div></div></body></html>"
    )
    return Response(html, mimetype="text/html")


@app.get("/alerts")
def alerts_history() -> Response:
    scanner = scanner_instance
    alerts = list(scanner.recent_alerts)[:50] if scanner else []
    return jsonify(alerts)


@app.get("/status")
def scanner_status() -> Response:
    scanner = scanner_instance
    payload = {
        "scanner_running": bool(scanner_thread and scanner_thread.is_alive()),
        "watchlist_size": len(scanner.daily_watchlist) if scanner else 0,
        "symbols_total": len(scanner.symbols) if scanner else 0,
        "last_scan_time": scanner.last_scan_time if scanner else None,
        "alerts_today": scanner.total_alerts_generated if scanner else 0,
    }
    return jsonify(payload)


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


@app.get("/test-alert")
def test_alert() -> Response:
    expected_token = env_value("TEST_ENDPOINT_TOKEN")
    provided_token = request.args.get("token", "").strip()

    if not expected_token:
        return Response("test endpoint is disabled", status=403)

    if provided_token != expected_token:
        return Response("unauthorized", status=401)

    config = load_config(CONFIG_PATH)
    scanner = IntradayStockScanner(config, [])
    sample_alert = {
        "symbol": "SBIN.NS",
        "price": 785.20,
        "signals": ["VWAP breakdown", "volume spike", "market bearish"],
        "score": 4,
        "priority": 6.80,
        "vwap": 790.10,
        "rsi": 28.40,
        "gap_percent": -1.20,
        "market_trend": "bearish",
        "higher_timeframe_trend": "bearish",
        "suggested_bias": "SHORT",
        "confluence_strength": "Strong",
        "intraday_change": -2.43,
    }
    message = scanner._format_alert_message(sample_alert)
    scanner._send_telegram_alert(message)
    return Response(message, status=200, mimetype="text/plain")


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
