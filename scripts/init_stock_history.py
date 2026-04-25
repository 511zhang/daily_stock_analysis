#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===================================
stock_daily 历史数据初始化脚本
===================================

功能：
- 拉取全市场 A 股近 N 个交易日的日线数据，写入 stock_daily 表
- 支持断点续传（默认跳过已有足够数据的股票）
- 计算 MA5/MA10/MA20、量比（volume_ratio）
- 并发拉取，内置限速，避免触发反爬

用法：
    # 默认：拉取 120 个交易日，5 个并发
    python scripts/init_stock_history.py

    # 自定义参数
    python scripts/init_stock_history.py --days 120 --workers 5

    # 强制重新拉取（忽略已有数据）
    python scripts/init_stock_history.py --force

    # 只拉取指定股票（调试用）
    python scripts/init_stock_history.py --codes 600519,000858,300750

注意：
- 全市场 ~5000 只股票，建议在非交易时段运行，避免占用实时行情接口配额
- 每只股票拉取间隔约 2-4 秒（防封禁），全量预计需要 3-5 小时
- 进度会写入 checkpoint 文件，Ctrl+C 中断后重新运行可断点续传
"""

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import List, Optional, Set, Tuple

import pandas as pd

# 确定项目根目录：
#   优先使用运行时工作目录（从 DSA 主库目录执行时），
#   回退到脚本所在项目根目录（独立运行时）。
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
logger = logging.getLogger("init_history")

# 进度检查点文件
CHECKPOINT_FILE = _CWD / "data" / ".init_history_checkpoint.json"


# ─────────────────────────────────────────────
# 技术指标计算
# ─────────────────────────────────────────────

def _calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    补全 MA5/MA10/MA20 和 volume_ratio（如果 fetcher 没有返回的话）。

    volume_ratio = 当日成交量 / 过去 5 日平均成交量
    """
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


# ─────────────────────────────────────────────
# 检查点读写
# ─────────────────────────────────────────────

def _load_checkpoint() -> Set[str]:
    """读取已完成的股票列表。"""
    if not CHECKPOINT_FILE.exists():
        return set()
    try:
        data = json.loads(CHECKPOINT_FILE.read_text())
        return set(data.get("done", []))
    except Exception:
        return set()


def _save_checkpoint(done: Set[str]) -> None:
    """保存已完成的股票列表。"""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        CHECKPOINT_FILE.write_text(json.dumps({"done": sorted(done), "updated_at": datetime.now().isoformat()}))
    except Exception as e:
        logger.warning("检查点写入失败: %s", e)


# ─────────────────────────────────────────────
# 单股票拉取
# ─────────────────────────────────────────────

def _fetch_one(
    code: str,
    manager,
    db,
    days: int,
    min_existing_days: int,
    force: bool,
) -> Tuple[str, str, int]:
    """
    拉取单只股票的历史数据并写入 DB。

    Returns:
        (code, status, saved_count)
        status: "skip" | "ok" | "fail"
    """
    try:
        # 检查是否已有足够数据（断点续传）
        if not force:
            with db.get_session() as session:
                from sqlalchemy import func, select
                from src.storage import StockDaily
                cnt = session.execute(
                    select(func.count(StockDaily.id)).where(StockDaily.code == code)
                ).scalar() or 0
            if cnt >= min_existing_days:
                return code, "skip", 0

        # 多拉 30 天作为 MA 预热窗口，计算完成后截断到目标天数
        fetch_days = days + 30
        df, source = manager.get_daily_data(code, days=fetch_days)

        if df is None or df.empty:
            return code, "fail", 0

        if "date" not in df.columns:
            return code, "fail", 0

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.sort_values("date").reset_index(drop=True)

        # 基于全量数据计算指标（保证 MA/量比不受截断影响）
        df = _calc_indicators(df)

        # 截断到最近 days 个交易日
        df = df.tail(days).reset_index(drop=True)

        saved = db.save_daily_data(df, code, data_source=source)
        return code, "ok", saved

    except Exception as e:
        logger.debug("[%s] 拉取失败: %s", code, e)
        return code, "fail", 0


# ─────────────────────────────────────────────
# 获取股票列表
# ─────────────────────────────────────────────

def _get_stock_list(codes_arg: Optional[str] = None) -> List[str]:
    """
    获取全市场 A 股代码列表，多来源自动切换。

    优先级：
      1. ak.stock_info_a_code_name()   — akshare，依赖深交所/上交所接口
      2. ak.stock_zh_a_spot_em()       — 东方财富全量实时行情，字段更全
      3. tushare pro.stock_basic()     — 需要 TUSHARE_TOKEN

    Args:
        codes_arg: 逗号分隔的股票代码（调试/测试用），None 表示全市场

    Returns:
        股票代码列表（纯6位数字，不含 SH/SZ 前缀）
    """
    if codes_arg:
        return [c.strip().lstrip("SHszsz") for c in codes_arg.split(",") if c.strip()]

    exclude_prefixes = ("688",)  # 科创板可按需包含

    def _filter(codes: List[str]) -> List[str]:
        return [
            c for c in codes
            if c and len(c) == 6 and c.isdigit()
            and not any(c.startswith(p) for p in exclude_prefixes)
        ]

    import akshare as ak

    # ── 方案 1：akshare stock_info_a_code_name ──────────────────────
    try:
        logger.info("获取股票列表：尝试 ak.stock_info_a_code_name()...")
        df = ak.stock_info_a_code_name()
        codes = df["code"].astype(str).str.zfill(6).tolist()
        result = _filter(codes)
        if result:
            logger.info("股票列表（方案1）获取成功：%d 只", len(result))
            return result
    except Exception as e:
        logger.warning("方案1 失败: %s，尝试方案2...", e)

    # ── 方案 2：东方财富全量实时行情 ────────────────────────────────
    try:
        logger.info("获取股票列表：尝试 ak.stock_zh_a_spot_em()...")
        df = ak.stock_zh_a_spot_em()
        codes = df["代码"].astype(str).str.zfill(6).tolist()
        result = _filter(codes)
        if result:
            logger.info("股票列表（方案2）获取成功：%d 只", len(result))
            return result
    except Exception as e:
        logger.warning("方案2 失败: %s，尝试方案3...", e)

    # ── 方案 3：Tushare stock_basic ─────────────────────────────────
    try:
        import os
        token = os.getenv("TUSHARE_TOKEN", "")
        if not token:
            raise ValueError("TUSHARE_TOKEN 未设置")
        import tushare as ts
        pro = ts.pro_api(token)
        logger.info("获取股票列表：尝试 tushare pro.stock_basic()...")
        df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol")
        # ts_code 格式: 000001.SZ -> 取前6位
        codes = df["ts_code"].str[:6].tolist()
        result = _filter(codes)
        if result:
            logger.info("股票列表（方案3）获取成功：%d 只", len(result))
            return result
    except Exception as e:
        logger.warning("方案3 失败: %s", e)

    # ── 方案 4：本地按交易所规则生成（无需网络）─────────────────────
    logger.warning("所有在线方案均失败，使用本地规则生成股票代码列表（会包含少量无效代码，拉取时自动跳过）")
    codes = []
    # 上交所主板：600000-600999, 601000-601999, 603000-603999, 605000-605999
    for prefix in ("600", "601", "603", "605"):
        codes += [f"{prefix}{str(i).zfill(3)}" for i in range(1000)]
    # 深交所主板：000001-001999
    for prefix in ("000", "001"):
        codes += [f"{prefix}{str(i).zfill(3)}" for i in range(1000)]
    # 深交所中小板：002xxx, 003xxx
    for prefix in ("002", "003"):
        codes += [f"{prefix}{str(i).zfill(3)}" for i in range(1000)]
    # 深交所创业板：300xxx, 301xxx
    for prefix in ("300", "301"):
        codes += [f"{prefix}{str(i).zfill(3)}" for i in range(1000)]
    result = _filter(codes)
    logger.info("本地生成股票代码：%d 个（含少量未上市代码，实际拉取时自动忽略）", len(result))
    return result


# ─────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="初始化 stock_daily 历史数据")
    parser.add_argument("--days", type=int, default=120, help="拉取交易日数量（默认 120）")
    parser.add_argument("--workers", type=int, default=5, help="并发线程数（默认 5）")
    parser.add_argument("--force", action="store_true", help="忽略已有数据，强制重新拉取")
    parser.add_argument("--codes", type=str, default=None, help="指定股票代码（逗号分隔，调试用）")
    parser.add_argument("--clear-checkpoint", action="store_true", help="清除断点续传记录，重新开始")
    args = parser.parse_args()

    # DataFetcherManager 初始化时自动注册全部数据源
    from data_provider import DataFetcherManager
    from src.storage import get_db

    manager = DataFetcherManager()
    db = get_db()

    # 获取股票列表
    all_codes = _get_stock_list(args.codes)

    # 断点续传
    if args.clear_checkpoint and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("已清除断点续传记录")

    done_set: Set[str] = set() if args.force else _load_checkpoint()
    pending = [c for c in all_codes if c not in done_set]

    logger.info(
        "待处理 %d 只股票（已完成 %d 只，跳过 %d 只）",
        len(pending), len(done_set), len(all_codes) - len(pending),
    )
    if not pending:
        logger.info("所有股票已处理完毕，退出")
        return

    # 并发拉取
    done_lock = Lock()
    counters = {"ok": 0, "skip": 0, "fail": 0, "total": 0}
    start_ts = time.time()
    min_existing = max(int(args.days * 0.8), 60)  # 已有 80% 天数则跳过

    def _worker(code: str):
        result = _fetch_one(code, manager, db, args.days, min_existing, args.force)
        return result

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(_worker, c): c for c in pending}

            for future in as_completed(futures):
                code, status, saved = future.result()
                counters[status] += 1
                counters["total"] += 1

                with done_lock:
                    if status in ("ok", "skip"):
                        done_set.add(code)

                # 每 50 只保存一次检查点
                if counters["total"] % 50 == 0:
                    _save_checkpoint(done_set)

                # 进度日志
                elapsed = time.time() - start_ts
                rate = counters["total"] / elapsed if elapsed > 0 else 0
                remaining = (len(pending) - counters["total"]) / rate if rate > 0 else 0
                status_icon = {"ok": "✓", "skip": "─", "fail": "✗"}.get(status, "?")
                logger.info(
                    "[%s] %-8s  进度 %d/%d  ✓%d ─%d ✗%d  速率 %.1f/min  预计剩余 %dm",
                    status_icon, code,
                    counters["total"], len(pending),
                    counters["ok"], counters["skip"], counters["fail"],
                    rate * 60,
                    int(remaining / 60),
                )

    except KeyboardInterrupt:
        logger.info("用户中断，保存进度...")

    _save_checkpoint(done_set)

    elapsed = time.time() - start_ts
    logger.info(
        "\n" + "=" * 60 +
        "\n初始化完成！耗时 %.0f 分钟" +
        "\n  成功写入: %d 只" +
        "\n  已跳过:   %d 只" +
        "\n  失败:     %d 只" +
        "\n  检查点:   %s" +
        "\n" + "=" * 60,
        elapsed / 60,
        counters["ok"],
        counters["skip"],
        counters["fail"],
        str(CHECKPOINT_FILE),
    )

    # 验证结果
    logger.info("正在验证数据库写入结果...")
    try:
        from sqlalchemy import text
        with db.get_session() as session:
            row = session.execute(
                text("SELECT COUNT(DISTINCT code) as codes, COUNT(*) as rows, "
                     "MIN(date) as min_d, MAX(date) as max_d FROM stock_daily")
            ).one()
            logger.info(
                "stock_daily 汇总：%d 只股票，%d 条记录，日期范围 %s ~ %s",
                row.codes, row.rows, row.min_d, row.max_d,
            )
    except Exception as e:
        logger.warning("验证查询失败: %s", e)


if __name__ == "__main__":
    main()
