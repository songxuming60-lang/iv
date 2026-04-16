"""
上期所全品种期权历史隐含波动率自动更新脚本
每次运行只补齐缺失日期，不重复拉取
"""

import akshare as ak
import pandas as pd
from datetime import date, timedelta
import time
import os

# ── 品种配置：名称 → (数据文件路径, 上市日期) ──────────────────────
SYMBOLS = {
    "铜期权":    ("data/copper_iv.xlsx",    date(2018,  9, 21)),
    "天然橡胶期权": ("data/rubber_iv.xlsx",    date(2019,  1, 28)),
    "黄金期权":   ("data/gold_iv.xlsx",      date(2019, 12, 20)),
    "铝期权":    ("data/aluminum_iv.xlsx",  date(2020,  8, 10)),
    "锌期权":    ("data/zinc_iv.xlsx",      date(2020,  8, 10)),
    "白银期权":   ("data/silver_iv.xlsx",    date(2022, 12, 26)),
    "铅期权":    ("data/lead_iv.xlsx",      date(2024,  9,  2)),
    "镍期权":    ("data/nickel_iv.xlsx",    date(2024,  9,  2)),
    "锡期权":    ("data/tin_iv.xlsx",       date(2024,  9,  2)),
    "燃料油期权":  ("data/fuel_iv.xlsx",     date(2025,  9, 10)),
}

SLEEP_SEC = 0.8
# ─────────────────────────────────────────────────────────────────


def get_weekdays(start: date, end: date) -> list[str]:
    dates, cur = [], start
    while cur <= end:
        if cur.weekday() < 5:
            dates.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return dates


def load_existing(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        return pd.read_excel(path, dtype=str)
    return pd.DataFrame()


def fetch_one(symbol: str, trade_date: str) -> pd.DataFrame | None:
    try:
        df = ak.option_vol_shfe(symbol=symbol, trade_date=trade_date)
        if df is not None and not df.empty:
            df.insert(0, "交易日期", trade_date)
            return df
    except Exception as e:
        print(f"    [跳过] {trade_date} 失败: {e}")
    return None


def process_iv(df: pd.DataFrame) -> pd.DataFrame:
    if "隐含波动率" in df.columns:
        df["隐含波动率"] = pd.to_numeric(df["隐含波动率"], errors="coerce")
        max_val = df["隐含波动率"].dropna().max()
        if pd.notna(max_val) and max_val < 5:
            df["隐含波动率(%)"] = (df["隐含波动率"] * 100).round(4)
        else:
            df["隐含波动率(%)"] = df["隐含波动率"].round(4)
    return df


def update_symbol(symbol: str, output_path: str, list_date: date):
    print(f"\n{'='*50}")
    print(f"品种: {symbol}  →  {output_path}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    existing = load_existing(output_path)

    if not existing.empty and "交易日期" in existing.columns:
        last_date = pd.to_datetime(existing["交易日期"]).max().date()
        start = last_date + timedelta(days=1)
        print(f"已有数据至 {last_date}，从 {start} 开始补充")
    else:
        start = list_date
        print(f"无历史数据，从上市日 {start} 开始全量拉取")

    end = date.today()
    if start > end:
        print("已是最新，无需更新")
        return

    trade_dates = get_weekdays(start, end)
    print(f"待拉取交易日: {len(trade_dates)} 个")

    new_records = []
    for i, td in enumerate(trade_dates, 1):
        print(f"  [{i:>3}/{len(trade_dates)}] {td} ...", end=" ")
        df = fetch_one(symbol, td)
        if df is not None:
            new_records.append(df)
            print(f"OK ({len(df)} 条)")
        else:
            print("无数据")
        time.sleep(SLEEP_SEC)

    if not new_records:
        print("本次无新数据")
        return

    new_df = pd.concat(new_records, ignore_index=True)
    new_df["交易日期"] = pd.to_datetime(
        new_df["交易日期"], format="%Y%m%d"
    ).dt.strftime("%Y-%m-%d")
    new_df = process_iv(new_df)

    if not existing.empty:
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined.drop_duplicates(subset=["交易日期", "合约系列"], keep="last", inplace=True)
        combined.sort_values("交易日期", inplace=True)
    else:
        combined = new_df

    combined.to_excel(output_path, index=False)
    print(f"已保存 {len(combined)} 条记录 → {output_path}")


def main():
    for symbol, (path, list_date) in SYMBOLS.items():
        update_symbol(symbol, path, list_date)
    print(f"\n{'='*50}")
    print("全部品种更新完成！")


if __name__ == "__main__":
    main()
