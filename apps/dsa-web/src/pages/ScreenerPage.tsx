import type React from 'react';
import { useState, useEffect, useCallback } from 'react';
import { RefreshCw, Search, TrendingUp, TrendingDown, Minus, Filter } from 'lucide-react';
import { screenerApi } from '../api/screener';
import type { PresetItem, ScreenerResultItem, ScreenerResponse } from '../api/screener';
import { getParsedApiError } from '../api/error';
import { ApiErrorAlert, Badge, Card, EmptyState } from '../components/common';

// ── Helpers ────────────────────────────────────────────────────────────────

function fmtPct(v?: number | null): string {
  if (v == null) return '--';
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
}

function fmtNum(v?: number | null, decimals = 2): string {
  if (v == null) return '--';
  return v.toFixed(decimals);
}

function fmtMv(v?: number | null): string {
  if (v == null) return '--';
  if (v >= 1e12) return `${(v / 1e12).toFixed(1)}万亿`;
  if (v >= 1e8) return `${(v / 1e8).toFixed(1)}亿`;
  if (v >= 1e4) return `${(v / 1e4).toFixed(0)}万`;
  return v.toFixed(0);
}

function PctCell({ value }: { value?: number | null }) {
  if (value == null) return <span className="text-secondary-text">--</span>;
  const up = value > 0;
  const down = value < 0;
  return (
    <span className={up ? 'text-[var(--color-up)]' : down ? 'text-[var(--color-down)]' : 'text-secondary-text'}>
      {up && <TrendingUp className="mr-0.5 inline h-3 w-3" />}
      {down && <TrendingDown className="mr-0.5 inline h-3 w-3" />}
      {!up && !down && <Minus className="mr-0.5 inline h-3 w-3" />}
      {fmtPct(value)}
    </span>
  );
}

// ── PresetCard ─────────────────────────────────────────────────────────────

function PresetCard({
  preset,
  active,
  onClick,
}: {
  preset: PresetItem;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        'w-full rounded-xl border px-3 py-2.5 text-left transition-all',
        active
          ? 'border-[hsl(var(--primary))] bg-[hsl(var(--primary)/0.1)] text-foreground'
          : 'border-border/50 bg-surface hover:border-border hover:bg-hover',
      ].join(' ')}
    >
      <p className="text-sm font-medium">{preset.name}</p>
      <p className="mt-0.5 text-xs text-secondary-text line-clamp-2">{preset.description}</p>
    </button>
  );
}

// ── ResultTable ────────────────────────────────────────────────────────────

const COLS = [
  { key: 'code', label: '代码', width: 'w-20' },
  { key: 'name', label: '名称', width: 'w-24' },
  { key: 'price', label: '最新价', width: 'w-20' },
  { key: 'changePct', label: '涨跌幅', width: 'w-24' },
  { key: 'volumeRatio', label: '量比', width: 'w-16' },
  { key: 'turnoverRate', label: '换手%', width: 'w-16' },
  { key: 'amplitude', label: '振幅%', width: 'w-16' },
  { key: 'circMv', label: '流通市值', width: 'w-24' },
  { key: 'peRatio', label: 'PE', width: 'w-16' },
  { key: 'change60d', label: '60日%', width: 'w-20' },
];

function ResultTable({ rows }: { rows: ScreenerResultItem[] }) {
  if (rows.length === 0) {
    return <EmptyState title="暂无结果" description="当前条件未筛选到符合股票" />;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border/50">
            {COLS.map((c) => (
              <th
                key={c.key}
                className={`${c.width} py-2 pr-3 text-left text-xs font-medium text-secondary-text first:pl-1`}
              >
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={row.code}
              className="border-b border-border/20 transition-colors hover:bg-hover"
            >
              <td className="py-2 pl-1 pr-3 font-mono text-xs text-secondary-text">{row.code}</td>
              <td className="py-2 pr-3 font-medium">{row.name ?? '--'}</td>
              <td className="py-2 pr-3">{fmtNum(row.price)}</td>
              <td className="py-2 pr-3">
                <PctCell value={row.changePct} />
              </td>
              <td className="py-2 pr-3">{fmtNum(row.volumeRatio)}</td>
              <td className="py-2 pr-3">{fmtNum(row.turnoverRate)}</td>
              <td className="py-2 pr-3">{fmtNum(row.amplitude)}</td>
              <td className="py-2 pr-3">{fmtMv(row.circMv)}</td>
              <td className="py-2 pr-3">{fmtNum(row.peRatio, 1)}</td>
              <td className="py-2 pr-3">
                <PctCell value={row.change60d} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Main Page ──────────────────────────────────────────────────────────────

const ScreenerPage: React.FC = () => {
  const [presets, setPresets] = useState<PresetItem[]>([]);
  const [activePreset, setActivePreset] = useState<string>('volume_surge');
  const [result, setResult] = useState<ScreenerResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ReturnType<typeof getParsedApiError> | null>(null);
  const [lastScanned, setLastScanned] = useState<string>('');

  // 加载预设列表
  useEffect(() => {
    screenerApi.getPresets().then((res) => setPresets(res.presets)).catch(() => {});
  }, []);

  const runScan = useCallback(async (preset: string) => {
    setLoading(true);
    setError(null);
    try {
      const res = await screenerApi.scanByPreset(preset, 100);
      setResult(res);
      setLastScanned(new Date().toLocaleTimeString('zh-CN'));
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setLoading(false);
    }
  }, []);

  // 初始加载
  useEffect(() => {
    void runScan(activePreset);
  }, [activePreset, runScan]);

  const handlePresetClick = (key: string) => {
    setActivePreset(key);
  };

  const handleRefresh = () => {
    void runScan(activePreset);
  };

  const currentPreset = presets.find((p) => p.key === activePreset);

  return (
    <div className="flex h-full flex-col gap-4 p-4 md:p-6">
      {/* 头部 */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Filter className="h-5 w-5 text-[hsl(var(--primary))]" />
          <h1 className="text-lg font-semibold">策略筛选</h1>
          {result && !result.cacheEmpty && (
            <Badge variant="default" className="text-xs">
              扫描 {result.totalScanned.toLocaleString()} 只
            </Badge>
          )}
        </div>
        <div className="flex items-center gap-2 text-xs text-secondary-text">
          {lastScanned && <span>更新于 {lastScanned}</span>}
          <button
            type="button"
            onClick={handleRefresh}
            disabled={loading}
            className="flex items-center gap-1 rounded-lg border border-border/50 px-2.5 py-1.5 text-xs hover:bg-hover disabled:opacity-50"
          >
            <RefreshCw className={['h-3.5 w-3.5', loading ? 'animate-spin' : ''].join(' ')} />
            刷新
          </button>
        </div>
      </div>

      {error ? <ApiErrorAlert error={error} /> : null}

      <div className="flex flex-1 gap-4 overflow-hidden">
        {/* 左侧预设列表 */}
        <div className="flex w-44 shrink-0 flex-col gap-2 overflow-y-auto pr-1">
          <p className="text-xs font-medium text-secondary-text">预设策略</p>
          {presets.map((p) => (
            <PresetCard
              key={p.key}
              preset={p}
              active={activePreset === p.key}
              onClick={() => handlePresetClick(p.key)}
            />
          ))}
        </div>

        {/* 右侧结果区 */}
        <Card className="flex flex-1 flex-col overflow-hidden p-0">
          {/* 策略说明条 */}
          {currentPreset && (
            <div className="flex items-center gap-2 border-b border-border/40 px-4 py-2.5">
              <Search className="h-4 w-4 shrink-0 text-[hsl(var(--primary))]" />
              <div className="min-w-0">
                <span className="font-medium text-sm">{currentPreset.name}</span>
                <span className="ml-2 text-xs text-secondary-text">{currentPreset.description}</span>
              </div>
              {result && (
                <Badge variant={result.totalMatched > 0 ? 'success' : 'default'} className="ml-auto shrink-0 text-xs">
                  {result.totalMatched} 只
                </Badge>
              )}
            </div>
          )}

          {/* 缓存为空提示 */}
          {result?.cacheEmpty && (
            <div className="flex flex-1 items-center justify-center p-8 text-center">
              <div>
                <p className="font-medium">行情缓存为空</p>
                <p className="mt-1 text-sm text-secondary-text">
                  实时行情缓存尚未建立，请先在交易时段运行系统，缓存调度器将自动每15分钟刷新行情数据。
                </p>
              </div>
            </div>
          )}

          {/* 加载中 */}
          {loading && (
            <div className="flex flex-1 items-center justify-center">
              <div className="h-6 w-6 animate-spin rounded-full border-2 border-cyan/20 border-t-cyan" />
            </div>
          )}

          {/* 结果表格 */}
          {!loading && !result?.cacheEmpty && result && (
            <div className="flex-1 overflow-auto px-4 pb-4 pt-2">
              <ResultTable rows={result.results} />
            </div>
          )}
        </Card>
      </div>
    </div>
  );
};

export default ScreenerPage;
