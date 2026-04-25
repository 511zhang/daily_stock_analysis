# -*- coding: utf-8 -*-
"""
===================================
行情缓存定时刷新模块
===================================

职责：
1. 盘中每 15 分钟批量拉取全市场 A 股实时行情，写入 realtime_quote_cache 表
2. 历史日线：stock_daily 表已由主流程写入，直接读取即可（本模块不重复处理）
3. 仅在交易时间段内触发刷新，非交易时段跳过以节省 API 配额

使用方式：
    在 main.py 中注册为后台任务：

        from data_provider.cache_scheduler import make_cache_refresh_task
        background_tasks.append({
            "task": make_cache_refresh_task(),
            "interval_seconds": 900,   # 15 分钟
            "run_immediately": True,
            "name": "realtime_cache_refresh",
        })

设计原则：
- 本模块只负责"写缓存"；读缓存由 akshare_fetcher._get_stock_realtime_quote_em 完成
- 刷新失败不抛出异常，仅记录日志，确保后台任务不影响主分析流程
- 非交易时段（包括周末/节假日）自动跳过，不产生无效 API 请求
"""

import logging
import time
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# 交易时段：09:25（集合竞价开始）到 15:05（收盘后缓冲）
_TRADING_START = (9, 25)
_TRADING_END = (15, 5)


def _is_trading_time() -> bool:
    """
    判断当前是否处于 A 股交易时段（工作日 09:25–15:05）。

    不做节假日日历校验（成本高），仅判断星期 + 时间窗口。
    节假日期间接口返回空数据，刷新任务会自然跳过写入。
    """
    now = datetime.now()
    if now.weekday() >= 5:  # 周六 = 5，周日 = 6
        return False
    h, m = now.hour, now.minute
    start_minutes = _TRADING_START[0] * 60 + _TRADING_START[1]
    end_minutes = _TRADING_END[0] * 60 + _TRADING_END[1]
    current_minutes = h * 60 + m
    return start_minutes <= current_minutes <= end_minutes


def _fetch_and_save_realtime_cache() -> int:
    """
    拉取全市场 A 股实时行情并批量写入 DB 缓存。

    Returns:
        写入记录数，失败返回 0
    """
    try:
        import akshare as ak
        from data_provider.realtime_types import safe_float
        from src.storage import get_db

        logger.info("[CacheRefresh] 开始刷新 A 股实时行情缓存...")
        t0 = time.time()

        df = ak.stock_zh_a_spot_em()

        elapsed = time.time() - t0
        if df is None or df.empty:
            logger.warning("[CacheRefresh] ak.stock_zh_a_spot_em 返回空数据，跳过写入")
            return 0

        logger.info(
            "[CacheRefresh] 拉取成功：%d 只股票，耗时 %.2fs，开始写入 DB...",
            len(df), elapsed,
        )

        records = []
        for _, r in df.iterrows():
            code = str(r.get('代码', '')).strip()
            if not code:
                continue
            records.append({
                'code': code,
                'name': str(r.get('名称', '')),
                'price': safe_float(r.get('最新价')),
                'change_pct': safe_float(r.get('涨跌幅')),
                'change_amount': safe_float(r.get('涨跌额')),
                'volume': safe_float(r.get('成交量')),
                'amount': safe_float(r.get('成交额')),
                'volume_ratio': safe_float(r.get('量比')),
                'turnover_rate': safe_float(r.get('换手率')),
                'amplitude': safe_float(r.get('振幅')),
                'open_price': safe_float(r.get('今开')),
                'high': safe_float(r.get('最高')),
                'low': safe_float(r.get('最低')),
                'pre_close': safe_float(r.get('昨收')),
                'pe_ratio': safe_float(r.get('市盈率-动态')),
                'pb_ratio': safe_float(r.get('市净率')),
                'total_mv': safe_float(r.get('总市值')),
                'circ_mv': safe_float(r.get('流通市值')),
                'change_60d': safe_float(r.get('60日涨跌幅')),
                'high_52w': safe_float(r.get('52周最高')),
                'low_52w': safe_float(r.get('52周最低')),
            })

        db = get_db()
        n = db.save_realtime_quote_batch(records, source="cache_scheduler")
        total_elapsed = time.time() - t0
        logger.info(
            "[CacheRefresh] 完成：写入 %d 条，总耗时 %.2fs",
            n, total_elapsed,
        )
        return n

    except Exception as e:
        logger.warning("[CacheRefresh] 刷新失败（后台任务继续）: %s", e)
        return 0


def make_cache_refresh_task(force: bool = False) -> Callable[[], None]:
    """
    返回一个可以注册到 Scheduler.add_background_task 的可调用对象。

    Args:
        force: 为 True 时忽略交易时段限制，强制刷新（测试/手动触发用）

    Returns:
        无参可调用函数
    """
    def _task() -> None:
        if not force and not _is_trading_time():
            logger.debug(
                "[CacheRefresh] 当前非交易时段 (%s)，跳过刷新",
                datetime.now().strftime("%H:%M"),
            )
            return
        _fetch_and_save_realtime_cache()

    return _task


def run_once(force: bool = False) -> int:
    """
    手动触发一次缓存刷新（命令行调试用）。

    Args:
        force: 忽略交易时段检查

    Returns:
        写入记录数
    """
    if not force and not _is_trading_time():
        logger.info(
            "[CacheRefresh] 当前非交易时段，使用 force=True 可强制刷新"
        )
        return 0
    return _fetch_and_save_realtime_cache()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    import sys
    force_flag = "--force" in sys.argv
    n = run_once(force=force_flag)
    print(f"写入 {n} 条实时行情缓存")
