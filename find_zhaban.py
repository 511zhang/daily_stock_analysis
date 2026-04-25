#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
炸板票筛选工具 v2
增加: 涨停原因、炸板次数、综合评分、问股话术
用法: python3 find_zhaban.py 通信
      python3 find_zhaban.py 芯片 算力
"""
import sys
import akshare as ak
import pandas as pd

_all_quotes = None
_limit_up_data = None

def get_all_quotes():
    global _all_quotes
    if _all_quotes is None:
        print("正在获取全市场行情数据...")
        _all_quotes = ak.stock_zh_a_spot_em()
        print(f"获取完成，共{len(_all_quotes)}只股票")
    return _all_quotes

def get_limit_up_reasons():
    global _limit_up_data
    if _limit_up_data is not None:
        return _limit_up_data

    reasons = {}
    today = pd.Timestamp.now().strftime('%Y%m%d')

    # 获取炸板股池
    try:
        print("正在获取炸板数据...")
        df = ak.stock_zt_pool_previous_em(date=today)
        for _, row in df.iterrows():
            code = str(row.get('代码', ''))
            if code:
                reasons[code] = {
                    'reason': str(row.get('涨停原因', '')),
                    'first_time': str(row.get('首次涨停时间', '')),
                    'last_time': str(row.get('最后涨停时间', '')),
                    'open_count': str(row.get('炸板次数', '')),
                    'status': '炸板'
                }
    except Exception as e:
        print(f"获取炸板数据异常: {e}")

    # 获取封板股池
    try:
        df2 = ak.stock_zt_pool_em(date=today)
        for _, row in df2.iterrows():
            code = str(row.get('代码', ''))
            if code:
                reasons[code] = {
                    'reason': str(row.get('涨停原因', '')),
                    'first_time': str(row.get('首次涨停时间', '')),
                    'last_time': str(row.get('最后涨停时间', '')),
                    'open_count': str(row.get('炸板次数', '0')),
                    'status': '封板'
                }
    except Exception as e:
        print(f"获取封板数据异常: {e}")

    print(f"涨停数据获取完成，共{len(reasons)}只")
    _limit_up_data = reasons
    return reasons

def find_zhaban(concept_name):
    print(f"\n{'='*60}")
    print(f"正在搜索概念: {concept_name}")
    print(f"{'='*60}")

    try:
        boards = ak.stock_board_concept_name_em()
        matched = boards[boards['板块名称'].str.contains(concept_name)]
        if matched.empty:
            print(f"未找到概念: {concept_name}")
            similar = boards[boards['板块名称'].str.contains(concept_name[0])]
            print("相似概念:")
            for _, row in similar.head(10).iterrows():
                print(f"  - {row['板块名称']}")
            return []
        board_name = matched.iloc[0]['板块名称']
        print(f"匹配到板块: {board_name}")
    except Exception as e:
        print(f"获取概念板块失败: {e}")
        return []

    try:
        stocks = ak.stock_board_concept_cons_em(symbol=board_name)
        stock_codes = set(stocks['代码'].tolist())
        print(f"板块内个股数量: {len(stock_codes)}")
    except Exception as e:
        print(f"获取成分股失败: {e}")
        return []

    quote = get_all_quotes()
    limit_reasons = get_limit_up_reasons()
    concept_stocks = quote[quote['代码'].isin(stock_codes)].copy()
    print(f"匹配到行情数据: {len(concept_stocks)}只")

    zhaban_list = []
    for _, row in concept_stocks.iterrows():
        try:
            code = str(row['代码'])
            name = str(row['名称'])
            close = float(row['最新价']) if pd.notna(row['最新价']) else 0
            high = float(row['最高']) if pd.notna(row['最高']) else 0
            prev_close = float(row['昨收']) if pd.notna(row['昨收']) else 0
            change_pct = float(row['涨跌幅']) if pd.notna(row['涨跌幅']) else 0
            turnover = float(row['换手率']) if pd.notna(row['换手率']) else 0
            volume_ratio = float(row['量比']) if pd.notna(row['量比']) else 0
            market_cap = float(row['流通市值']) if pd.notna(row['流通市值']) else 0

            if prev_close <= 0 or close <= 0:
                continue

            if name.startswith(('ST', '*ST')):
                limit_price = round(prev_close * 1.05, 2)
            else:
                limit_price = round(prev_close * 1.10, 2)

            touched_limit = high >= limit_price - 0.02
            not_locked = close < limit_price - 0.02

            if touched_limit and not_locked:
                gap = round((limit_price - close) / limit_price * 100, 2)
                cap_yi = round(market_cap / 100000000, 1)

                reason_info = limit_reasons.get(code, {})
                reason = reason_info.get('reason', '未知')
                open_count = reason_info.get('open_count', '未知')
                first_time = reason_info.get('first_time', '')
                last_time = reason_info.get('last_time', '')

                zhaban_list.append({
                    '代码': code,
                    '名称': name,
                    '收盘价': close,
                    '涨停价': limit_price,
                    '涨跌幅': round(change_pct, 2),
                    '距涨停': f"{gap}%",
                    '换手率': round(turnover, 2),
                    '量比': round(volume_ratio, 2),
                    '流通市值(亿)': cap_yi,
                    '涨停原因': reason,
                    '炸板次数': open_count,
                    '首次涨停': first_time,
                    '末次涨停': last_time,
                })
        except Exception:
            continue

    if not zhaban_list:
        print(f"\n{concept_name} 概念今天没有炸板票")
        return []

    zhaban_list.sort(key=lambda x: x['涨跌幅'], reverse=True)

    print(f"\n{'='*60}")
    print(f"{concept_name} 概念炸板票（共{len(zhaban_list)}只）")
    print(f"{'='*60}")

    codes = []
    for i, item in enumerate(zhaban_list, 1):
        print(f"\n{i}. {item['名称']}({item['代码']})")
        print(f"   涨停原因: {item['涨停原因']}")
        print(f"   涨跌幅: {item['涨跌幅']}% | 距涨停: {item['距涨停']}")
        print(f"   换手率: {item['换手率']}% | 量比: {item['量比']}")
        print(f"   流通市值: {item['流通市值(亿)']}亿")
        print(f"   炸板次数: {item['炸板次数']} | 首次涨停: {item['首次涨停']} | 末次涨停: {item['末次涨停']}")

        score = 50
        if item['涨跌幅'] >= 7:
            score += 15
            strength = "强势"
        elif item['涨跌幅'] >= 5:
            score += 5
            strength = "中性"
        else:
            score -= 10
            strength = "弱势"

        if 5 <= item['换手率'] <= 10:
            score += 5
        elif item['换手率'] > 15:
            score -= 10

        if item['炸板次数'] not in ('未知', ''):
            try:
                oc = int(item['炸板次数'])
                if oc <= 1:
                    score += 5
                elif oc >= 3:
                    score -= 10
            except ValueError:
                pass

        if 30 <= item['流通市值(亿)'] <= 150:
            score += 5

        verdict = ""
        if score >= 70:
            verdict = ">>> 重点关注!"
        elif score >= 55:
            verdict = ">>> 可以关注"
        else:
            verdict = ">>> 建议回避"

        print(f"   综合评分: {score}分 | {strength}炸板 {verdict}")
        codes.append(item['代码'])

    print(f"\n{'='*60}")
    print(f"复制到问股使用:")
    print(f"{','.join(codes)}")
    print(f"\n推荐问股话术:")
    names = [f"{item['名称']}({item['代码']})" for item in zhaban_list]
    print(f"以下是{concept_name}概念今天炸板的票：{', '.join(names)}")
    print(f"请用涨停炸板策略逐一分析，选出最强一只，给出次日操作计划。")
    print(f"{'='*60}")

    return codes


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python3 find_zhaban.py 概念名称")
        print("示例: python3 find_zhaban.py 通信")
        print("      python3 find_zhaban.py 芯片 算力")
        sys.exit(1)

    all_codes = []
    for concept in sys.argv[1:]:
        codes = find_zhaban(concept)
        all_codes.extend(codes)

    if all_codes:
        print(f"\n所有概念炸板票汇总（共{len(all_codes)}只）:")
        print(f"{','.join(all_codes)}")
