"""
期权历史隐含波动率自动更新脚本
- 上期所品种：使用 AKShare option_vol_shfe 接口
- 广期所品种：使用 GFEX 官网接口
"""

import akshare as ak
import pandas as pd
import requests
from datetime import date, timedelta
import time
import os

# ── 上期所品种配置 ─────────────────────────────────────────────────
SYMBOLS_SHFE = {
    "铜期权":     ("data/copper_iv.csv",    date(2018,  9, 21)),
    "天胶期权":   ("data/rubber_iv.csv",    date(2019,  1, 28)),
    "黄金期权":   ("data/gold_iv.csv",      date(2019, 12, 20)),
    "铝期权":     ("data/aluminum_iv.csv",  date(2020,  8, 10)),
    "锌期权":     ("data/zinc_iv.csv",      date(2020,  8, 10)),
    "白银期权":   ("data/silver_iv.csv",    date(2022, 12, 26)),
    "铅期权":     ("data/lead_iv.csv",      date(2024,  9,  2)),
    "镍期权":     ("data/nickel_iv.csv",    date(2024,  9,  2)),
    "锡期权":     ("data/tin_iv.csv",       date(2024,  9,  2)),
    "燃料油期权": ("data/fuel_iv.csv",      date(2025,  9, 10)),
}

# ── 广期所品种配置 ─────────────────────────────────────────────────
SYMBOLS_GFEX = {
    "工业硅期权": ("data/si_iv.csv",  "si", date(2022, 12, 23)),
    "碳酸锂期权": ("data/lc_iv.csv",  "lc", date(2023,  7, 24)),
    "多晶硅期权": ("data/ps_iv.csv",  "ps", date(2024, 12, 27)),
    "铂期权":     ("data/pt_iv.csv",  "pt", date(2025, 11, 28)),
    "钯期权":     ("data/pd_iv.csv",  "pd", date(2025, 11, 28)),
}

SLEEP_SEC = 0.3

# ── 广期所接口配置 ─────────────────────────────────────────────────
_GFEX_URL = "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadListOptVolatility"
_GFEX_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "www.gfex.com.cn",
    "Origin": "http://www.gfex.com.cn",
    "Referer": "http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "X-Requested-With": "XMLHttpRequest",
}
_GFEX_COL_MAP = {
    "seriesId":        "合约系列",
    "volume":          "成交量",
    "openInterest":    "持仓量",
    "openInterestChg": "持仓量变化",
    "turnover":        "成交额",
    "exerciseVolume":  "行权量",
    "hisVolatility":   "隐含波动率",
}
# ─────────────────────────────────────────────────────────────────


def get_weekdays(start: date, end: date) -> list[str]:
    dates, cur = [], start
    while cur <= end:
        if cur.weekday() < 5:
            dates.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return dates


def load_existing(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"  >> 文件不存在: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, dtype=str, encoding='utf-8-sig')
        if df.empty or "交易日期" not in df.columns:
            return pd.DataFrame()
        df["交易日期"] = pd.to_datetime(df["交易日期"], errors='coerce').dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["交易日期"])
        print(f"  >> 已有数据: {len(df)} 条，最新日期: {df['交易日期'].max()}")
        return df
    except Exception as e:
        print(f"  >> 读取失败: {e}")
        return pd.DataFrame()


def save_data(existing: pd.DataFrame, new_df: pd.DataFrame, output_path: str):
    if not existing.empty:
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined.drop_duplicates(subset=["交易日期", "合约系列"], keep="last", inplace=True)
        combined.sort_values("交易日期", inplace=True)
    else:
        combined = new_df
    combined.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  已保存 {len(combined)} 条 → {output_path}")


def process_iv(df: pd.DataFrame) -> pd.DataFrame:
    if "隐含波动率" in df.columns:
        df["隐含波动率"] = pd.to_numeric(df["隐含波动率"], errors="coerce")
        max_val = df["隐含波动率"].dropna().max()
        if pd.notna(max_val) and max_val < 5:
            df["隐含波动率(%)"] = (df["隐含波动率"] * 100).round(4)
        else:
            df["隐含波动率(%)"] = df["隐含波动率"].round(4)
    return df


# ── 上期所更新 ─────────────────────────────────────────────────────
def fetch_one_shfe(symbol: str, trade_date: str) -> pd.DataFrame | None:
    try:
        df = ak.option_vol_shfe(symbol=symbol, trade_date=trade_date)
        if df is not None and not df.empty:
            df.insert(0, "交易日期", trade_date)
            return df
    except Exception:
        pass
    return None


def update_shfe(symbol: str, output_path: str, list_date: date):
    print(f"\n{'='*50}")
    print(f"[上期所] {symbol}  →  {output_path}")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    existing = load_existing(output_path)

    if not existing.empty:
        try:
            last_date = pd.to_datetime(existing["交易日期"]).max().date()
            start = last_date + timedelta(days=1)
            print(f"  已有数据至 {last_date}，从 {start} 开始补充")
        except:
            start = list_date
    else:
        start = list_date
        print(f"  无历史数据，从上市日 {start} 开始全量拉取")

    end = date.today()
    if start > end:
        print(f"  已是最新，跳过")
        return

    trade_dates = get_weekdays(start, end)
    print(f"  待拉取交易日: {len(trade_dates)} 个")

    new_records = []
    for i, td in enumerate(trade_dates, 1):
        print(f"  [{i:>4}/{len(trade_dates)}] {td} ...", end=" ", flush=True)
        df = fetch_one_shfe(symbol, td)
        if df is not None:
            new_records.append(df)
            print(f"OK ({len(df)} 条)")
        else:
            print("无数据")
        time.sleep(SLEEP_SEC)

    if not new_records:
        print(f"  本次无新数据")
        return

    new_df = pd.concat(new_records, ignore_index=True)
    new_df["交易日期"] = pd.to_datetime(new_df["交易日期"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    new_df = process_iv(new_df)
    save_data(existing, new_df, output_path)


# ── 广期所更新 ─────────────────────────────────────────────────────
def fetch_raw_gfex(trade_date: str) -> list | None:
    try:
        resp = requests.post(_GFEX_URL, data={"trade_date": trade_date},
                             headers=_GFEX_HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        print(f"    [请求失败] {e}")
        return None


def update_gfex(symbol: str, output_path: str, code: str, list_date: date):
    print(f"\n{'='*50}")
    print(f"[广期所] {symbol}  →  {output_path}")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    existing = load_existing(output_path)

    if not existing.empty:
        try:
            last_date = pd.to_datetime(existing["交易日期"]).max().date()
            start = last_date + timedelta(days=1)
            print(f"  已有数据至 {last_date}，从 {start} 开始补充")
        except:
            start = list_date
    else:
        start = list_date
        print(f"  无历史数据，从上市日 {start} 开始全量拉取")

    end = date.today()
    if start > end:
        print(f"  已是最新，跳过")
        return

    trade_dates = get_weekdays(start, end)
    print(f"  待拉取交易日: {len(trade_dates)} 个")

    new_records = []
    for i, td in enumerate(trade_dates, 1):
        print(f"  [{i:>4}/{len(trade_dates)}] {td} ...", end=" ", flush=True)
        raw = fetch_raw_gfex(td)
        if raw is None:
            print("请求失败")
            time.sleep(SLEEP_SEC)
            continue
        if not raw:
            print("无数据")
            time.sleep(SLEEP_SEC)
            continue
        df = pd.DataFrame(raw)
        df = df[df["seriesId"].str.lower().str.startswith(code)].copy()
        if df.empty:
            print("无匹配数据")
            time.sleep(SLEEP_SEC)
            continue
        df = df.rename(columns=_GFEX_COL_MAP)
        df.insert(0, "交易日期", td)
        new_records.append(df)
        print(f"OK ({len(df)} 条)")
        time.sleep(SLEEP_SEC)

    if not new_records:
        print(f"  本次无新数据")
        return

    new_df = pd.concat(new_records, ignore_index=True)
    new_df["交易日期"] = pd.to_datetime(new_df["交易日期"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    new_df = process_iv(new_df)
    save_data(existing, new_df, output_path)


# ── 主程序 ─────────────────────────────────────────────────────────
def main():
    print(f"工作目录: {os.getcwd()}")
    print(f"上期所品种: {len(SYMBOLS_SHFE)}，广期所品种: {len(SYMBOLS_GFEX)}")

    for sym, (path, ld) in SYMBOLS_SHFE.items():
        update_shfe(sym, path, ld)

    for sym, (path, code, ld) in SYMBOLS_GFEX.items():
        update_gfex(sym, path, code, ld)

    print(f"\n{'='*50}")
    print("全部品种更新完成！")


if __name__ == "__main__":
    main()
