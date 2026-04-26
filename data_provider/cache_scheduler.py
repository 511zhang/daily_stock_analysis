# -*- coding: utf-8 -*-
"""
===================================
行情缓存定时刷新模块
===================================

职责：
1. 盘中每 3 分钟批量拉取全市场 A 股实时行情，写入 realtime_quote_cache 表
2. 双数据源策略：新浪批量接口（快，~8秒全市场）为主，东方财富（字段全）为辅
3. 仅在交易时间段内触发刷新，非交易时段跳过以节省 API 配额

使用方式：
    在 main.py 中注册为后台任务：

        from data_provider.cache_scheduler import make_cache_refresh_task
        background_tasks.append({
            "task": make_cache_refresh_task(),
            "interval_seconds": 180,   # 3 分钟
            "run_immediately": True,
            "name": "realtime_cache_refresh",
        })

数据源策略：
- 新浪 hq.sinajs.cn 批量接口：速度极快（800只/请求，全市场<10秒），周末也可用
  字段：名称、今开、昨收、最新价、最高、最低、成交量、成交额
  缺少：量比、换手率、PE、PB、市值、60日涨跌幅、52周高低
- 东方财富 ak.stock_zh_a_spot_em()：字段齐全，但周末/节假日接口不可用
  包含上述所有字段

策略：每次先尝试东方财富（字段全），失败后自动降级到新浪（速度快、稳定）
"""

import logging
import random
import time
from datetime import datetime
from typing import Callable, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# 交易时段：09:15（盘前）到 15:05（收盘后缓冲）
_TRADING_START = (9, 15)
_TRADING_END = (15, 5)

# 新浪批量接口配置
_SINA_ENDPOINT = "hq.sinajs.cn"
_SINA_BATCH_SIZE = 800
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
]


def _is_trading_time() -> bool:
    """
    判断当前是否处于 A 股交易时段（工作日 09:15–15:05）。

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


# ─────────────────────────────────────────────
# 新浪批量接口
# ─────────────────────────────────────────────

def _code_to_sina_symbol(code: str) -> str:
    """6位代码 -> 新浪格式: sh600519 / sz000001 / bj920748"""
    c = code.strip()
    if c.startswith(("6", "5")):
        return f"sh{c}"
    elif c.startswith(("0", "3")):
        return f"sz{c}"
    elif c.startswith(("4", "8", "9")):
        return f"bj{c}"
    return f"sz{c}"


def _safe_float(s) -> Optional[float]:
    try:
        v = float(s)
        return v if v == v else None
    except (ValueError, TypeError):
        return None


def _fetch_sina_batch(codes: List[str]) -> Dict[str, dict]:
    """
    通过新浪 hq.sinajs.cn 批量接口获取实时行情。

    Args:
        codes: 6位股票代码列表

    Returns:
        {code: {name, price, open_price, pre_close, high, low, volume, amount, change_pct, ...}}
    """
    symbols = [_code_to_sina_symbol(c) for c in codes]
    url = f"http://{_SINA_ENDPOINT}/list={','.join(symbols)}"
    headers = {
        'Referer': 'http://finance.sina.com.cn',
        'User-Agent': random.choice(_USER_AGENTS),
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.encoding = 'gbk'

    results = {}
    for line in r.text.strip().split('\n'):
        if '=""' in line or '"' not in line:
            continue
        var_part = line.split('=')[0]
        sym = var_part.replace('var hq_str_', '').strip()
        code = sym[2:]  # 去掉 sh/sz/bj 前缀

        data = line.split('"')[1].split(',')
        if len(data) < 32:
            continue

        price = _safe_float(data[3])
        if not price or price <= 0:
            continue

        pre_close = _safe_float(data[2])
        high = _safe_float(data[4])
        low = _safe_float(data[5])

        change_pct = None
        change_amount = None
        amplitude = None
        if pre_close and pre_close > 0:
            change_pct = round((price - pre_close) / pre_close * 100, 4)
            change_amount = round(price - pre_close, 4)
            if high and low:
                amplitude = round((high - low) / pre_close * 100, 2)

        results[code] = {
            'code': code,
            'name': data[0],
            'price': price,
            'change_pct': change_pct,
            'change_amount': change_amount,
            'volume': _safe_float(data[8]),
            'amount': _safe_float(data[9]),
            'amplitude': amplitude,
            'open_price': _safe_float(data[1]),
            'high': high,
            'low': low,
            'pre_close': pre_close,
        }
    return results


def _fetch_all_sina() -> List[dict]:
    """
    通过新浪批量接口获取全市场实时行情。

    Returns:
        记录列表，每条包含 cache 表所需字段
    """
    from src.storage import get_db
    from sqlalchemy import text

    db = get_db()
    with db.get_session() as s:
        codes = [r[0] for r in s.execute(
            text("SELECT DISTINCT code FROM stock_daily ORDER BY code")
        ).fetchall()]

    if not codes:
        logger.warning("[CacheRefresh/Sina] stock_daily 为空，无法获取股票列表")
        return []

    all_results = {}
    for i in range(0, len(codes), _SINA_BATCH_SIZE):
        batch = codes[i:i + _SINA_BATCH_SIZE]
        try:
            results = _fetch_sina_batch(batch)
            all_results.update(results)
        except Exception as e:
            logger.warning("[CacheRefresh/Sina] 批次 %d 失败: %s", i // _SINA_BATCH_SIZE + 1, e)
        time.sleep(0.3)

    return list(all_results.values())


# ─────────────────────────────────────────────
# 东方财富全量接口（字段更丰富）
# ─────────────────────────────────────────────

def _fetch_all_eastmoney() -> List[dict]:
    """
    通过东方财富 ak.stock_zh_a_spot_em() 获取全市场行情。

    字段比新浪更全（含量比、换手率、PE、PB、市值等），
    但周末/节假日接口不可用。
    """
    import akshare as ak
    from data_provider.realtime_types import safe_float

    df = ak.stock_zh_a_spot_em()
    if df is None or df.empty:
        return []

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
    return records


# ─────────────────────────────────────────────
# 统一刷新入口
# ─────────────────────────────────────────────

def _fetch_and_save_realtime_cache() -> int:
    """
    拉取全市场 A 股实时行情并批量写入 DB 缓存。

    策略：先尝试东方财富（字段全），失败后降级到新浪（速度快、稳定性好）。

    Returns:
        写入记录数，失败返回 0
    """
    from src.storage import get_db

    t0 = time.time()
    records = []
    source = "unknown"

    # 尝试东方财富（字段最全）
    try:
        logger.info("[CacheRefresh] 尝试东方财富全量接口...")
        records = _fetch_all_eastmoney()
        if records:
            source = "eastmoney"
            logger.info("[CacheRefresh] 东方财富成功：%d 只，耗时 %.1fs", len(records), time.time() - t0)
    except Exception as e:
        logger.info("[CacheRefresh] 东方财富失败: %s，降级到新浪接口", e)

    # 降级到新浪
    if not records:
        try:
            logger.info("[CacheRefresh] 使用新浪批量接口...")
            t1 = time.time()
            records = _fetch_all_sina()
            source = "sina"
            if records:
                logger.info("[CacheRefresh] 新浪成功：%d 只，耗时 %.1fs", len(records), time.time() - t1)
            else:
                logger.warning("[CacheRefresh] 新浪返回空数据")
        except Exception as e:
            logger.warning("[CacheRefresh] 新浪也失败: %s", e)

    if not records:
        logger.warning("[CacheRefresh] 所有数据源均失败，跳过本次刷新")
        return 0

    # 写入数据库
    try:
        db = get_db()
        n = db.save_realtime_quote_batch(records, source=f"cache_scheduler_{source}")
        total_elapsed = time.time() - t0
        logger.info("[CacheRefresh] 完成：写入 %d 条 (源=%s)，总耗时 %.1fs", n, source, total_elapsed)
        return n
    except Exception as e:
        logger.warning("[CacheRefresh] 写入失败: %s", e)
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
