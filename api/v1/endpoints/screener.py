# -*- coding: utf-8 -*-
"""
===================================
策略筛选 API 端点
===================================

职责：
1. 提供策略筛选接口
2. 列出可用预设策略
3. 支持自定义条件组合筛选
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.v1.schemas.screener import (
    PresetListResponse,
    ScreenerFilterRequest,
    ScreenerRequest,
    ScreenerResponse,
)
from src.services.screener_service import (
    PresetStrategy,
    ScreenerFilter,
    list_presets,
    run_screening,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/presets", response_model=PresetListResponse)
async def get_presets():
    """获取所有预设筛选策略"""
    return PresetListResponse(presets=list_presets())


@router.post("/scan", response_model=ScreenerResponse)
async def scan_stocks(req: ScreenerRequest):
    """
    执行策略筛选。

    - 指定 preset 使用预设策略
    - 指定 filter 使用自定义条件
    - 两者都不指定则返回全部（按涨幅排序）
    """
    preset_enum = None
    if req.preset:
        try:
            preset_enum = PresetStrategy(req.preset)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"未知预设策略: {req.preset}，可用: {[p.value for p in PresetStrategy]}",
            )

    custom_filter = None
    if req.filter:
        custom_filter = ScreenerFilter(
            change_pct_min=req.filter.change_pct_min,
            change_pct_max=req.filter.change_pct_max,
            volume_ratio_min=req.filter.volume_ratio_min,
            volume_ratio_max=req.filter.volume_ratio_max,
            turnover_rate_min=req.filter.turnover_rate_min,
            turnover_rate_max=req.filter.turnover_rate_max,
            pe_ratio_min=req.filter.pe_ratio_min,
            pe_ratio_max=req.filter.pe_ratio_max,
            pb_ratio_min=req.filter.pb_ratio_min,
            pb_ratio_max=req.filter.pb_ratio_max,
            circ_mv_min=req.filter.circ_mv_min,
            circ_mv_max=req.filter.circ_mv_max,
            amplitude_min=req.filter.amplitude_min,
            amplitude_max=req.filter.amplitude_max,
            price_min=req.filter.price_min,
            price_max=req.filter.price_max,
            near_52w_high_pct=req.filter.near_52w_high_pct,
            near_52w_low_pct=req.filter.near_52w_low_pct,
            change_60d_min=req.filter.change_60d_min,
            change_60d_max=req.filter.change_60d_max,
        )

    result = run_screening(
        preset=preset_enum,
        custom_filter=custom_filter,
        sort_by=req.sort_by,
        sort_desc=req.sort_desc,
        limit=req.limit,
    )
    return ScreenerResponse(**result)


@router.get("/scan/{preset_key}", response_model=ScreenerResponse)
async def scan_by_preset(
    preset_key: str,
    limit: int = Query(50, ge=1, le=200),
    sort_by: Optional[str] = Query(None),
):
    """快捷接口：按预设策略名筛选"""
    try:
        preset_enum = PresetStrategy(preset_key)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"未知预设策略: {preset_key}",
        )

    result = run_screening(
        preset=preset_enum,
        sort_by=sort_by,
        limit=limit,
    )
    return ScreenerResponse(**result)
