# -*- coding: utf-8 -*-
"""
===================================
策略筛选 API Schema
===================================
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ScreenerFilterRequest(BaseModel):
    """自定义筛选条件"""
    change_pct_min: Optional[float] = Field(None, description="涨跌幅下限(%)")
    change_pct_max: Optional[float] = Field(None, description="涨跌幅上限(%)")
    volume_ratio_min: Optional[float] = Field(None, description="量比下限")
    volume_ratio_max: Optional[float] = Field(None, description="量比上限")
    turnover_rate_min: Optional[float] = Field(None, description="换手率下限(%)")
    turnover_rate_max: Optional[float] = Field(None, description="换手率上限(%)")
    pe_ratio_min: Optional[float] = Field(None, description="PE下限")
    pe_ratio_max: Optional[float] = Field(None, description="PE上限")
    pb_ratio_min: Optional[float] = Field(None, description="PB下限")
    pb_ratio_max: Optional[float] = Field(None, description="PB上限")
    circ_mv_min: Optional[float] = Field(None, description="流通市值下限(元)")
    circ_mv_max: Optional[float] = Field(None, description="流通市值上限(元)")
    amplitude_min: Optional[float] = Field(None, description="振幅下限(%)")
    amplitude_max: Optional[float] = Field(None, description="振幅上限(%)")
    price_min: Optional[float] = Field(None, description="股价下限")
    price_max: Optional[float] = Field(None, description="股价上限")
    near_52w_high_pct: Optional[float] = Field(None, description="距52周新高百分比以内")
    near_52w_low_pct: Optional[float] = Field(None, description="距52周新低百分比以内")
    change_60d_min: Optional[float] = Field(None, description="60日涨跌幅下限(%)")
    change_60d_max: Optional[float] = Field(None, description="60日涨跌幅上限(%)")


class ScreenerRequest(BaseModel):
    """筛选请求"""
    preset: Optional[str] = Field(None, description="预设策略名称")
    filter: Optional[ScreenerFilterRequest] = Field(None, description="自定义筛选条件")
    sort_by: Optional[str] = Field(None, description="排序字段")
    sort_desc: bool = Field(True, description="是否降序")
    limit: int = Field(50, ge=1, le=200, description="返回条数上限")


class StrategyInfo(BaseModel):
    """策略信息"""
    key: str
    name: str
    description: str


class ScreenerResponse(BaseModel):
    """筛选响应"""
    results: List[Dict[str, Any]]
    total_matched: int
    total_scanned: int
    strategy: Optional[StrategyInfo] = None
    scanned_at: str
    cache_empty: bool = False


class PresetItem(BaseModel):
    """预设策略条目"""
    key: str
    name: str
    description: str


class PresetListResponse(BaseModel):
    """预设策略列表响应"""
    presets: List[PresetItem]
