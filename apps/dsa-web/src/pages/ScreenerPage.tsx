import type React from 'react';
import { useState, useEffect, useCallback } from 'react';
import { RefreshCw, Zap, TrendingUp, TrendingDown, Minus } from 'lucide-react';
import { screenerApi } from '../api/screener';
import type { ScreenerResultItem, ScreenerResponse } from '../api/screener';
import { getParsedApiError } from '../api/error';
import { ApiErrorAlert, Badge, Card } from '../components/common';

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
  if (v >= 1e8) return `${(v / 1e8).toFixed(0)}亿`;
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

// ── Table ─────────────────────────────────────────────────────────────────

const COLS = [
  { key: 'code', label: '代码', width: 'w-20' },
  { key: 'name', label: '名称', width: 'w-24' },
  { key: 'price', label: '最新价', width: 'w-20' },
  { key: 'changePct', label: '今日涨跌', width: 'w-24' },
  { key: 'change3d', label: '3日%', width: 'w-20' },
  { key: 'change5d', label: '5日%', width: 'w-20' },
  { key: 'volumeRatio', label: '量比', width: 'w-16' },
  { key: 'turnoverRate', label: '换手%', width: 'w-16' },
  { key: 'circMv', label: '流通市值', width: 'w-24' },
  { key: 'amplitude', label: '振幅%', width: 'w-16' },
];

function ResultTable({ rows }: { rows: ScreenerResultItem[] }) {
  if (rows.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-secondary-text">
        <p className="text-sm">当前条件未筛选到股票</p>
        <p className="mt-1 text-xs">交易时段数据每3分钟自动更新</p>
      </div>
    );
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
              <td className="py-2 pr-3"><PctCell value={row.changePct} /></td>
              <td className="py-2 pr-3"><PctCell value={row.change3d} /></td>
              <td className="py-2 pr-3"><PctCell value={row.change5d} /></td>
              <td className="py-2 pr-3">{fmtNum(row.volumeRatio)}</td>
              <td className="py-2 pr-3">{fmtNum(row.turnoverRate)}</td>
              <td className="py-2 pr-3">{fmtMv(row.circMv)}</td>
              <td className="py-2 pr-3">{fmtNum(row.amplitude)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Main Page ──────────────────────────────────────────────────────────────

const ScreenerPage: React.FC = () => {
  const [result, setResult] = useState<ScreenerResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ReturnType<typeof getParsedApiError> | null>(null);
  const [lastScanned, setLastScanned] = useState<string>('');

  const runScan = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await screenerApi.scanByPreset('short_line', 100);
      setResult(res);
      setLastScanned(new Date().toLocaleTimeString('zh-CN'));
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void runScan();
  }, [runScan]);

  return (
    <div className="flex h-full flex-col gap-4 p-4 md:p-6">
      {/* 头部 */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Zap className="h-5 w-5 text-amber-500" />
          <h1 className="text-lg font-semibold">短线强势</h1>
          <span className="text-xs text-secondary-text">
            涨幅3-8% · 量比≥2 · 换手≥3% · 市值&gt;30亿
          </span>
          {result && !result.cacheEmpty && (
            <Badge variant={result.totalMatched > 0 ? 'success' : 'default'} className="text-xs">
              {result.totalMatched} 只
            </Badge>
          )}
        </div>
        <div className="flex items-center gap-2 text-xs text-secondary-text">
          {lastScanned && <span>更新于 {lastScanned}</span>}
          <button
            type="button"
            onClick={() => void runScan()}
            disabled={loading}
            className="flex items-center gap-1 rounded-lg border border-border/50 px-2.5 py-1.5 text-xs hover:bg-hover disabled:opacity-50"
          >
            <RefreshCw className={['h-3.5 w-3.5', loading ? 'animate-spin' : ''].join(' ')} />
            刷新
          </button>
        </div>
      </div>

      {error ? <ApiErrorAlert error={error} /> : null}

      <Card className="flex flex-1 flex-col overflow-hidden p-0">
        {/* 缓存为空 */}
        {result?.cacheEmpty && (
          <div className="flex flex-1 items-center justify-center p-8 text-center">
            <div>
              <p className="font-medium">行情缓存为空</p>
              <p className="mt-1 text-sm text-secondary-text">
                缓存每3分钟自动刷新，交易时段开始后数据将自动填充。
              </p>
            </div>
          </div>
        )}

        {/* 加载中 */}
        {loading && !result && (
          <div className="flex flex-1 items-center justify-center">
            <div className="h-6 w-6 animate-spin rounded-full border-2 border-amber-500/20 border-t-amber-500" />
          </div>
        )}

        {/* 结果 */}
        {!result?.cacheEmpty && result && (
          <div className="flex-1 overflow-auto px-4 pb-4 pt-2">
            <ResultTable rows={result.results} />
          </div>
        )}
      </Card>
    </div>
  );
};

export default ScreenerPage;
