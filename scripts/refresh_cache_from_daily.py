#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速补全 stock_daily 最近几天数据 + 更新 realtime_quote_cache。

用法:
    # 在容器里: python refresh_cache_from_daily.py --workers 8
    # 宿主机:   python scripts/refresh_cache_from_daily.py --workers 8 --limit 100
"""

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock

import pandas as pd

_CWD = Path.cwd()
_SCRIPT_ROOT = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _CWD if (_CWD / "src" / "storage.py").exists() else _SCRIPT_ROOT
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("refresh_cache")


def _calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").copy()
    if "close" in df.columns:
        if df.get("ma5") is None or df["ma5"].isna().all():
            df["ma5"] = df["close"].rolling(5, min_periods=1).mean().round(4)
        if df.get("ma10") is None or df["ma10"].isna().all():
            df["ma10"] = df["close"].rolling(10, min_periods=1).mean().round(4)
        if df.get("ma20") is None or df["ma20"].isna().all():
            df["ma20"] = df["close"].rolling(20, min_periods=1).mean().round(4)
    if "volume" in df.columns:
        if df.get("volume_ratio") is None or df["volume_ratio"].isna().all():
            vol_ma5 = df["volume"].rolling(5, min_periods=1).mean()
            df["volume_ratio"] = (df["volume"] / vol_ma5.replace(0, float("nan"))).round(4)
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=10, help="Fetch this many recent trading days per stock")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent threads")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of stocks (0=all)")
    args = parser.parse_args()

    from data_provider import DataFetcherManager
    from src.storage import RealtimeQuoteCache, get_db
    from sqlalchemy import text

    manager = DataFetcherManager()
    db = get_db()

    # Get all stock codes from stock_daily
    with db.get_session() as s:
        codes = [r[0] for r in s.execute(text("SELECT DISTINCT code FROM stock_daily")).fetchall()]
    logger.info("Total stocks in DB: %d", len(codes))

    if args.limit > 0:
        codes = codes[:args.limit]
        logger.info("Limited to %d stocks", len(codes))

    lock = Lock()
    results = {}  # code -> {latest row data}
    counters = {"ok": 0, "fail": 0, "total": 0}
    start_ts = time.time()

    def fetch_one(code):
        try:
            df, source = manager.get_daily_data(code, days=args.days + 30)
            if df is None or df.empty or "date" not in df.columns:
                return code, "fail", None

            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df.sort_values("date").reset_index(drop=True)
            df = _calc_indicators(df)
            df = df.tail(args.days).reset_index(drop=True)

            saved = db.save_daily_data(df, code, data_source=source)

            # Get latest row for cache
            latest = df.iloc[-1].to_dict()
            prev_close = df.iloc[-2]["close"] if len(df) >= 2 else None
            latest["_prev_close"] = prev_close
            latest["_source"] = source

            return code, "ok", latest
        except Exception as e:
            logger.debug("[%s] fail: %s", code, e)
            return code, "fail", None

    logger.info("Starting fetch with %d workers...", args.workers)

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(fetch_one, c): c for c in codes}
            for future in as_completed(futures):
                code, status, data = future.result()
                counters[status] += 1
                counters["total"] += 1

                if data:
                    with lock:
                        results[code] = data

                if counters["total"] % 100 == 0:
                    elapsed = time.time() - start_ts
                    rate = counters["total"] / elapsed * 60 if elapsed > 0 else 0
                    logger.info(
                        "Progress %d/%d  ok=%d fail=%d  %.0f/min",
                        counters["total"], len(codes),
                        counters["ok"], counters["fail"], rate,
                    )
    except KeyboardInterrupt:
        logger.info("Interrupted.")

    elapsed = time.time() - start_ts
    logger.info(
        "Fetch done: %d ok, %d fail, %.0fs elapsed",
        counters["ok"], counters["fail"], elapsed,
    )

    # Update realtime_quote_cache with latest data
    if results:
        logger.info("Updating realtime_quote_cache with %d stocks...", len(results))
        now = datetime.now()
        with db.session_scope() as session:
            session.execute(text("DELETE FROM realtime_quote_cache"))
            for code, data in results.items():
                pc = data.get("_prev_close")
                price = data.get("close")
                high = data.get("high")
                low = data.get("low")
                change_amount = round(price - pc, 4) if pc and price and pc > 0 else None
                amplitude = round((high - low) / pc * 100, 2) if pc and high and low and pc > 0 else None

                obj = RealtimeQuoteCache(
                    code=code,
                    name='',
                    price=price,
                    change_pct=data.get("pct_chg"),
                    change_amount=change_amount,
                    volume=data.get("volume"),
                    amount=data.get("amount"),
                    volume_ratio=data.get("volume_ratio"),
                    amplitude=amplitude,
                    open_price=data.get("open"),
                    high=high,
                    low=low,
                    pre_close=pc,
                    data_source=f"refresh_{data.get('date')}",
                    fetched_at=now,
                )
                session.add(obj)
        logger.info("Cache updated.")

    # Verify
    with db.get_session() as s:
        cnt = s.execute(text("SELECT COUNT(*) FROM realtime_quote_cache")).scalar()
        logger.info("realtime_quote_cache: %d rows", cnt)
        dates = s.execute(text(
            "SELECT data_source, COUNT(*) as cnt FROM realtime_quote_cache "
            "GROUP BY data_source ORDER BY cnt DESC LIMIT 5"
        )).fetchall()
        for r in dates:
            logger.info("  %s: %d", r.data_source, r.cnt)


if __name__ == "__main__":
    main()
