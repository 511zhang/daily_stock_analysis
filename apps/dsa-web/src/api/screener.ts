import apiClient from './index';
import { toCamelCase } from './utils';

export interface ScreenerFilterRequest {
  changePctMin?: number;
  changePctMax?: number;
  volumeRatioMin?: number;
  volumeRatioMax?: number;
  turnoverRateMin?: number;
  turnoverRateMax?: number;
  peRatioMin?: number;
  peRatioMax?: number;
  pbRatioMin?: number;
  pbRatioMax?: number;
  circMvMin?: number;
  circMvMax?: number;
  amplitudeMin?: number;
  amplitudeMax?: number;
  priceMin?: number;
  priceMax?: number;
  near52wHighPct?: number;
  near52wLowPct?: number;
  change60dMin?: number;
  change60dMax?: number;
}

export interface ScreenerRequest {
  preset?: string;
  filter?: ScreenerFilterRequest;
  sortBy?: string;
  sortDesc?: boolean;
  limit?: number;
}

export interface ScreenerResultItem {
  code: string;
  name?: string;
  price?: number;
  changePct?: number;
  changeAmount?: number;
  volume?: number;
  amount?: number;
  volumeRatio?: number;
  turnoverRate?: number;
  amplitude?: number;
  openPrice?: number;
  high?: number;
  low?: number;
  preClose?: number;
  peRatio?: number;
  pbRatio?: number;
  totalMv?: number;
  circMv?: number;
  change60d?: number;
  high52w?: number;
  low52w?: number;
}

export interface StrategyInfo {
  key: string;
  name: string;
  description: string;
}

export interface ScreenerResponse {
  results: ScreenerResultItem[];
  totalMatched: number;
  totalScanned: number;
  strategy?: StrategyInfo;
  scannedAt: string;
  cacheEmpty: boolean;
}

export interface PresetItem {
  key: string;
  name: string;
  description: string;
}

export interface PresetListResponse {
  presets: PresetItem[];
}

export const screenerApi = {
  getPresets: async (): Promise<PresetListResponse> => {
    const res = await apiClient.get('/api/v1/screener/presets');
    return toCamelCase<PresetListResponse>(res.data);
  },

  scan: async (req: ScreenerRequest): Promise<ScreenerResponse> => {
    const payload: Record<string, unknown> = {
      sort_desc: req.sortDesc ?? true,
      limit: req.limit ?? 50,
    };
    if (req.preset) payload.preset = req.preset;
    if (req.sortBy) payload.sort_by = req.sortBy;
    if (req.filter) {
      const f = req.filter;
      payload.filter = {
        change_pct_min: f.changePctMin,
        change_pct_max: f.changePctMax,
        volume_ratio_min: f.volumeRatioMin,
        volume_ratio_max: f.volumeRatioMax,
        turnover_rate_min: f.turnoverRateMin,
        turnover_rate_max: f.turnoverRateMax,
        pe_ratio_min: f.peRatioMin,
        pe_ratio_max: f.peRatioMax,
        pb_ratio_min: f.pbRatioMin,
        pb_ratio_max: f.pbRatioMax,
        circ_mv_min: f.circMvMin,
        circ_mv_max: f.circMvMax,
        amplitude_min: f.amplitudeMin,
        amplitude_max: f.amplitudeMax,
        price_min: f.priceMin,
        price_max: f.priceMax,
        near_52w_high_pct: f.near52wHighPct,
        near_52w_low_pct: f.near52wLowPct,
        change_60d_min: f.change60dMin,
        change_60d_max: f.change60dMax,
      };
    }
    const res = await apiClient.post('/api/v1/screener/scan', payload);
    return toCamelCase<ScreenerResponse>(res.data);
  },

  scanByPreset: async (presetKey: string, limit = 50): Promise<ScreenerResponse> => {
    const res = await apiClient.get(`/api/v1/screener/scan/${presetKey}`, {
      params: { limit },
    });
    return toCamelCase<ScreenerResponse>(res.data);
  },
};
