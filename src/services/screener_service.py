# -*- coding: utf-8 -*-
"""
===================================
策略筛选服��
===================================

职责：
1. 基于 realtime_quote_cache 的量化数据做批量股票筛选
2. 提供多种预设筛选策略（量比异动、涨幅领先、52周新高等）
3. 支持自定义组合条件筛选
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from src.storage import get_db

logger = logging.getLogger(__name__)


class PresetStrategy(str, Enum):
    """预设筛选策略"""
    SHORT_LINE = "short_line"                # 短线强势（主推策略）
    VOLUME_SURGE = "volume_surge"            # 量比异动
    HIGH_TURNOVER = "high_turnover"          # 高换手
    PRICE_BREAKOUT = "price_breakout"        # 放量突破（接近52周新高）
    OVERSOLD_REBOUND = "oversold_rebound"    # 超跌反弹候选
    STRONG_MOMENTUM = "strong_momentum"      # 强势动能
    LOW_PE_VALUE = "low_pe_value"            # 低PE价值
    LIMIT_UP_BOARD = "limit_up_board"        # 涨停板
    LARGE_CAP_ACTIVE = "large_cap_active"    # 大盘股活跃
    ZHABAN = "zhaban"                        # 炸板票（曾触及涨停但未封���）


@dataclass
class ScreenerFilter:
    """筛选条件"""
    change_pct_min: Optional[float] = None
    change_pct_max: Optional[float] = None
    volume_ratio_min: Optional[float] = None
    volume_ratio_max: Optional[float] = None
    turnover_rate_min: Optional[float] = None
    turnover_rate_max: Optional[float] = None
    pe_ratio_min: Optional[float] = None
    pe_ratio_max: Optional[float] = None
    pb_ratio_min: Optional[float] = None
    pb_ratio_max: Optional[float] = None
    circ_mv_min: Optional[float] = None      # 流通市值下限（元）
    circ_mv_max: Optional[float] = None
    amplitude_min: Optional[float] = None
    amplitude_max: Optional[float] = None
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    near_52w_high_pct: Optional[float] = None  # 距52周新高百分比以内
    near_52w_low_pct: Optional[float] = None   # 距52周新低百分比以内
    change_60d_min: Optional[float] = None
    change_60d_max: Optional[float] = None


# 预设策略的筛选条件
PRESET_FILTERS: Dict[PresetStrategy, ScreenerFilter] = {
    PresetStrategy.SHORT_LINE: ScreenerFilter(
        change_pct_min=3.0,
        change_pct_max=8.0,             # 强势但没涨停封死
        volume_ratio_min=2.0,           # 明显放量
        turnover_rate_min=3.0,          # 流动性好
        circ_mv_min=3_000_000_000,      # 流通市值 > 30亿
    ),
    PresetStrategy.VOLUME_SURGE: ScreenerFilter(
        volume_ratio_min=3.0,
        change_pct_min=1.0,
        circ_mv_min=2_000_000_000,      # 流通市值 > 20亿
    ),
    PresetStrategy.HIGH_TURNOVER: ScreenerFilter(
        turnover_rate_min=8.0,
        change_pct_min=0.0,
        circ_mv_min=2_000_000_000,
    ),
    PresetStrategy.PRICE_BREAKOUT: ScreenerFilter(
        near_52w_high_pct=5.0,          # 距52周新高5%以内
        volume_ratio_min=1.5,
        change_pct_min=1.0,
        circ_mv_min=3_000_000_000,
    ),
    PresetStrategy.OVERSOLD_REBOUND: ScreenerFilter(
        change_60d_max=-20.0,           # 60日跌幅超过20%
        change_pct_min=2.0,             # 今日反弹2%以上
        volume_ratio_min=1.3,
        circ_mv_min=2_000_000_000,
    ),
    PresetStrategy.STRONG_MOMENTUM: ScreenerFilter(
        change_pct_min=5.0,
        volume_ratio_min=2.0,
        turnover_rate_min=3.0,
        circ_mv_min=3_000_000_000,
    ),
    PresetStrategy.LOW_PE_VALUE: ScreenerFilter(
        pe_ratio_min=1.0,
        pe_ratio_max=15.0,
        change_pct_min=-1.0,
        circ_mv_min=10_000_000_000,     # 流通市值 > 100亿
    ),
    PresetStrategy.LIMIT_UP_BOARD: ScreenerFilter(
        change_pct_min=9.8,
    ),
    PresetStrategy.LARGE_CAP_ACTIVE: ScreenerFilter(
        circ_mv_min=50_000_000_000,     # 流通市值 > 500亿
        volume_ratio_min=1.5,
        turnover_rate_min=1.0,
    ),
}

# 预设策略描述
PRESET_DESCRIPTIONS: Dict[PresetStrategy, Dict[str, str]] = {
    PresetStrategy.SHORT_LINE: {
        "name": "短线强势",
        "description": "涨幅3-8%、量比≥2、换手≥3%、市值>30亿，放量强势股",
    },
    PresetStrategy.VOLUME_SURGE: {
        "name": "量比异动",
        "description": "量比≥3 且上涨，流通市值>20亿，捕捉资金异动信号",
    },
    PresetStrategy.HIGH_TURNOVER: {
        "name": "高换手率",
        "description": "换手率≥8% 且不下跌，流通市值>20亿，关注活跃度极高个股",
    },
    PresetStrategy.PRICE_BREAKOUT: {
        "name": "突破新高",
        "description": "股价距52周新高5%以内，放量上涨，流通市值>30亿",
    },
    PresetStrategy.OVERSOLD_REBOUND: {
        "name": "超跌反弹",
        "description": "60日跌幅>20% 且今日反弹>2%，放量，捕捉超跌修复机会",
    },
    PresetStrategy.STRONG_MOMENTUM: {
        "name": "强势动能",
        "description": "涨幅≥5%、量比≥2、换手≥3%，流通市值>30亿，强势领涨股",
    },
    PresetStrategy.LOW_PE_VALUE: {
        "name": "低PE价值",
        "description": "PE 1-15倍，流通市值>100亿，低估值蓝筹��",
    },
    PresetStrategy.LIMIT_UP_BOARD: {
        "name": "涨停板",
        "description": "涨幅≥9.8%，捕捉涨停个股",
    },
    PresetStrategy.LARGE_CAP_ACTIVE: {
        "name": "大盘活跃",
        "description": "流通市值>500亿、量比≥1.5、换手≥1%，大盘股异动",
    },
    PresetStrategy.ZHABAN: {
        "name": "炸板票",
        "description": "盘中触及涨停但未封住，涨幅≥5%，捕捉强势回落机会",
    },
}


def _safe_float(val: Any) -> Optional[float]:
    """安全转换为 float，None / NaN / 非数字 → None"""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN check
            return None
        return f
    except (ValueError, TypeError):
        return None


def _match_filter(quote: Dict[str, Any], f: ScreenerFilter) -> bool:
    """判断单只股票是否匹配筛选��件"""
    price = _safe_float(quote.get('price'))
    change_pct = _safe_float(quote.get('change_pct'))
    volume_ratio = _safe_float(quote.get('volume_ratio'))
    turnover_rate = _safe_float(quote.get('turnover_rate'))
    pe_ratio = _safe_float(quote.get('pe_ratio'))
    pb_ratio = _safe_float(quote.get('pb_ratio'))
    circ_mv = _safe_float(quote.get('circ_mv'))
    amplitude = _safe_float(quote.get('amplitude'))
    change_60d = _safe_float(quote.get('change_60d'))
    high_52w = _safe_float(quote.get('high_52w'))
    low_52w = _safe_float(quote.get('low_52w'))

    # 基本有效性：必须有价格
    if price is None or price <= 0:
        return False

    # 涨跌幅
    if f.change_pct_min is not None:
        if change_pct is None or change_pct < f.change_pct_min:
            return False
    if f.change_pct_max is not None:
        if change_pct is None or change_pct > f.change_pct_max:
            return False

    # 量比
    if f.volume_ratio_min is not None:
        if volume_ratio is None or volume_ratio < f.volume_ratio_min:
            return False
    if f.volume_ratio_max is not None:
        if volume_ratio is None or volume_ratio > f.volume_ratio_max:
            return False

    # 换手率（数据缺失时跳过）
    if turnover_rate is not None:
        if f.turnover_rate_min is not None and turnover_rate < f.turnover_rate_min:
            return False
        if f.turnover_rate_max is not None and turnover_rate > f.turnover_rate_max:
            return False

    # PE（数据缺失时跳过）
    if pe_ratio is not None:
        if f.pe_ratio_min is not None and pe_ratio < f.pe_ratio_min:
            return False
        if f.pe_ratio_max is not None and pe_ratio > f.pe_ratio_max:
            return False

    # PB（数据缺失时跳过）
    if pb_ratio is not None:
        if f.pb_ratio_min is not None and pb_ratio < f.pb_ratio_min:
            return False
        if f.pb_ratio_max is not None and pb_ratio > f.pb_ratio_max:
            return False

    # 流通市值（数据缺失时跳过此条件，避免因缓存无市值数据导致全部过滤）
    if circ_mv is not None:
        if f.circ_mv_min is not None and circ_mv < f.circ_mv_min:
            return False
        if f.circ_mv_max is not None and circ_mv > f.circ_mv_max:
            return False

    # 振幅
    if f.amplitude_min is not None:
        if amplitude is None or amplitude < f.amplitude_min:
            return False
    if f.amplitude_max is not None:
        if amplitude is None or amplitude > f.amplitude_max:
            return False

    # 价格
    if f.price_min is not None:
        if price < f.price_min:
            return False
    if f.price_max is not None:
        if price > f.price_max:
            return False

    # 距52周新高
    if f.near_52w_high_pct is not None:
        if high_52w is None or high_52w <= 0:
            return False
        pct_from_high = (high_52w - price) / high_52w * 100
        if pct_from_high > f.near_52w_high_pct:
            return False

    # 距52周新低
    if f.near_52w_low_pct is not None:
        if low_52w is None or low_52w <= 0:
            return False
        pct_from_low = (price - low_52w) / low_52w * 100
        if pct_from_low > f.near_52w_low_pct:
            return False

    # 60日涨跌幅
    if f.change_60d_min is not None:
        if change_60d is None or change_60d < f.change_60d_min:
            return False
    if f.change_60d_max is not None:
        if change_60d is None or change_60d > f.change_60d_max:
            return False

    return True


def _is_zhaban(quote: Dict[str, Any]) -> bool:
    """判断炸板：盘中触及涨停价但收盘未封住"""
    price = _safe_float(quote.get('price'))
    high = _safe_float(quote.get('high'))
    pre_close = _safe_float(quote.get('pre_close'))
    name = quote.get('name', '')

    if not price or not high or not pre_close or pre_close <= 0:
        return False

    # ST股涨停幅度 5%，普通股 10%
    is_st = 'ST' in name
    limit_ratio = 1.05 if is_st else 1.10
    limit_price = round(pre_close * limit_ratio, 2)

    touched_limit = high >= limit_price - 0.02
    not_locked = price < limit_price - 0.02

    return touched_limit and not_locked


def _zhaban_score(quote: Dict[str, Any]) -> float:
    """炸板票评分（越高越值得关注）"""
    score = 50.0
    change_pct = _safe_float(quote.get('change_pct')) or 0
    turnover = _safe_float(quote.get('turnover_rate')) or 0
    circ_mv = _safe_float(quote.get('circ_mv')) or 0
    circ_mv_yi = circ_mv / 1e8

    if change_pct >= 7:
        score += 15
    elif change_pct >= 5:
        score += 5
    else:
        score -= 10

    if 5 <= turnover <= 10:
        score += 5
    elif turnover > 15:
        score -= 10

    if 30 <= circ_mv_yi <= 150:
        score += 5

    return score


def _sort_key_for_preset(preset: PresetStrategy, quote: Dict[str, Any]) -> float:
    """预设策略的排序键（越大越靠前）"""
    if preset == PresetStrategy.SHORT_LINE:
        # 综合评分：量比权重40% + 涨幅权重30% + 换手率权重30%
        vr = _safe_float(quote.get('volume_ratio')) or 0
        chg = _safe_float(quote.get('change_pct')) or 0
        tr = _safe_float(quote.get('turnover_rate')) or 0
        return vr * 4 + chg * 3 + tr * 3
    elif preset == PresetStrategy.VOLUME_SURGE:
        return _safe_float(quote.get('volume_ratio')) or 0
    elif preset == PresetStrategy.HIGH_TURNOVER:
        return _safe_float(quote.get('turnover_rate')) or 0
    elif preset == PresetStrategy.PRICE_BREAKOUT:
        high_52w = _safe_float(quote.get('high_52w'))
        price = _safe_float(quote.get('price'))
        if high_52w and price and high_52w > 0:
            return -((high_52w - price) / high_52w * 100)  # 越接近新高越靠前
        return -999
    elif preset == PresetStrategy.OVERSOLD_REBOUND:
        return _safe_float(quote.get('change_pct')) or 0
    elif preset == PresetStrategy.STRONG_MOMENTUM:
        return _safe_float(quote.get('change_pct')) or 0
    elif preset == PresetStrategy.LOW_PE_VALUE:
        pe = _safe_float(quote.get('pe_ratio'))
        return -(pe if pe else 999)  # PE越低越靠前
    elif preset == PresetStrategy.LIMIT_UP_BOARD:
        return _safe_float(quote.get('change_pct')) or 0
    elif preset == PresetStrategy.LARGE_CAP_ACTIVE:
        return _safe_float(quote.get('volume_ratio')) or 0
    elif preset == PresetStrategy.ZHABAN:
        return _zhaban_score(quote)
    return 0


def run_screening(
    preset: Optional[PresetStrategy] = None,
    custom_filter: Optional[ScreenerFilter] = None,
    sort_by: Optional[str] = None,
    sort_desc: bool = True,
    limit: int = 50,
    cache_max_age: int = 3600,
) -> Dict[str, Any]:
    """
    执行策略筛选。

    Args:
        preset: 预设策略名
        custom_filter: 自定义筛选条件（与 preset 二选一，custom_filter 优先）
        sort_by: 排序字段名（如 change_pct, volume_ratio 等）
        sort_desc: 是否降序
        limit: 返回条数上限
        cache_max_age: 缓存最大有效秒数

    Returns:
        {"results": [...], "total_matched": N, "total_scanned": N, "strategy": {...}, "scanned_at": ...}
    """
    db = get_db()
    all_quotes = db.get_all_realtime_cache(max_age_seconds=cache_max_age)

    if not all_quotes:
        return {
            "results": [],
            "total_matched": 0,
            "total_scanned": 0,
            "strategy": None,
            "scanned_at": datetime.now().isoformat(),
            "cache_empty": True,
        }

    # 决定用哪个 filter
    active_filter = custom_filter
    if active_filter is None and preset is not None:
        active_filter = PRESET_FILTERS.get(preset)

    if active_filter is None:
        # 没有条件，返回所有（按涨幅排序）
        active_filter = ScreenerFilter()

    # 过滤
    is_zhaban_mode = (preset == PresetStrategy.ZHABAN and custom_filter is None)
    matched = []
    for code, quote in all_quotes.items():
        # 跳过退市股
        name = quote.get('name', '')
        if '退' in name:
            continue
        # 炸板模式允许 ST 股（ST 有5%涨停），其他模式跳过 ST
        if not is_zhaban_mode and 'ST' in name:
            continue

        if is_zhaban_mode:
            if _is_zhaban(quote):
                matched.append(quote)
        elif _match_filter(quote, active_filter):
            matched.append(quote)

    # 排序
    if sort_by and matched and sort_by in matched[0]:
        matched.sort(
            key=lambda q: _safe_float(q.get(sort_by)) or (float('-inf') if sort_desc else float('inf')),
            reverse=sort_desc,
        )
    elif preset is not None:
        matched.sort(key=lambda q: _sort_key_for_preset(preset, q), reverse=True)
    else:
        matched.sort(
            key=lambda q: _safe_float(q.get('change_pct')) or float('-inf'),
            reverse=True,
        )

    strategy_info = None
    if preset is not None:
        desc = PRESET_DESCRIPTIONS.get(preset, {})
        strategy_info = {
            "key": preset.value,
            "name": desc.get("name", preset.value),
            "description": desc.get("description", ""),
        }

    return {
        "results": matched[:limit],
        "total_matched": len(matched),
        "total_scanned": len(all_quotes),
        "strategy": strategy_info,
        "scanned_at": datetime.now().isoformat(),
        "cache_empty": False,
    }


def list_presets() -> List[Dict[str, str]]:
    """列��所有预设策略"""
    result = []
    for preset in PresetStrategy:
        desc = PRESET_DESCRIPTIONS.get(preset, {})
        result.append({
            "key": preset.value,
            "name": desc.get("name", preset.value),
            "description": desc.get("description", ""),
        })
    return result


def format_screening_markdown(
    result: Dict[str, Any],
    max_rows: int = 20,
) -> str:
    """将筛选结果格式化为 Markdown，用于通知推送"""
    strategy = result.get("strategy")
    items = result.get("results", [])
    total = result.get("total_matched", 0)
    scanned = result.get("total_scanned", 0)
    scanned_at = result.get("scanned_at", "")

    title = strategy["name"] if strategy else "自定义筛选"
    desc = strategy.get("description", "") if strategy else ""

    lines = [
        f"## 策略筛选: {title}",
        f"> {desc}" if desc else "",
        f"> 扫描 {scanned} 只 | 命中 {total} 只 | {scanned_at}",
        "",
        "| 代码 | 名称 | 最新价 | 涨跌幅 | 量比 | 换手% | 流通市值 |",
        "|------|------|--------|--------|------|-------|----------|",
    ]

    for item in items[:max_rows]:
        code = item.get("code", "")
        name = item.get("name", "")
        price = item.get("price")
        chg = item.get("change_pct")
        vr = item.get("volume_ratio")
        tr = item.get("turnover_rate")
        mv = item.get("circ_mv")

        price_s = f"{price:.2f}" if price else "--"
        chg_s = f"{chg:+.2f}%" if chg is not None else "--"
        vr_s = f"{vr:.2f}" if vr is not None else "--"
        tr_s = f"{tr:.2f}" if tr is not None else "--"
        if mv and mv >= 1e8:
            mv_s = f"{mv / 1e8:.1f}亿"
        else:
            mv_s = "--"

        lines.append(f"| {code} | {name} | {price_s} | {chg_s} | {vr_s} | {tr_s} | {mv_s} |")

    if total > max_rows:
        lines.append(f"\n*...及其余 {total - max_rows} 只*")

    return "\n".join(lines)
