"""
05_get_data_check.py — API 路線驗證 / 小規模探針工具

指定一個城市 + 一個模型 + 一個日期 + 一個 endpoint，
做小規模檢查：確認 API 可用、欄位正確、回傳格式符合後續需求。

這支不是全量抓取器，是 probe 工具。

用法：
  python 05_get_data_check.py --city London --lat 51.5074 --lon -0.1278 --tz Europe/London --model gfs_seamless --date 2025-01-01
  python 05_get_data_check.py --city London --lat 51.5074 --lon -0.1278 --tz Europe/London --model gfs_seamless --date 2025-01-01 --api previous_runs
  python 05_get_data_check.py --city London --lat 51.5074 --lon -0.1278 --tz Europe/London --model gfs_seamless --date 2025-01-01 --save-raw --verbose
"""

import argparse
import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ============================================================
# 常數
# ============================================================

PROJ_DIR = Path(__file__).resolve().parent

# Open-Meteo 兩個 endpoint
ENDPOINTS = {
    "historical_forecast": "https://historical-forecast-api.open-meteo.com/v1/forecast",
    "previous_runs": "https://previous-runs-api.open-meteo.com/v1/forecast",
}

DEFAULT_API = "historical_forecast"

# hourly 變數：day0 + previous_day1~7
HOURLY_VARS = ["temperature_2m"]
for _i in range(1, 8):
    HOURLY_VARS.append(f"temperature_2m_previous_day{_i}")

# 預設輸出檔名
DEFAULT_SUMMARY_CSV = "05_probe_summary.csv"
DEFAULT_RAW_JSON = "05_probe_raw.json"

# 網路設定
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2  # 秒

# CSV 欄位
SUMMARY_FIELDS = [
    "date", "city", "model", "source_api", "available",
    "hourly_count", "daily_high_c", "daily_low_c",
    "vars_found", "vars_null_only", "vars_missing", "note",
]

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ============================================================
# API 請求
# ============================================================

def probe_endpoint(
    api_name: str,
    lat: float,
    lon: float,
    tz: str,
    model: str,
    target_date: str,
    verbose: bool = False,
) -> tuple[Optional[dict], Optional[str]]:
    """
    對一個 endpoint 發 request，帶 retry。
    回傳 (data_dict_or_None, error_str_or_None)。
    """
    import requests

    url = ENDPOINTS.get(api_name)
    if not url:
        return None, f"未知的 API 名稱: {api_name}（可選: {list(ENDPOINTS.keys())}）"

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "start_date": target_date,
        "end_date": target_date,
        "models": model,
        "timezone": tz,
    }

    if verbose:
        log.info(f"URL: {url}")
        log.info(f"params: {json.dumps(params, indent=2)}")

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if verbose:
                log.info(f"HTTP {resp.status_code} (attempt {attempt})")

            data = resp.json()

            if resp.status_code != 200:
                reason = data.get("reason", data.get("error", str(data)))
                last_error = f"HTTP {resp.status_code}: {reason}"
                if resp.status_code >= 500:
                    time.sleep(RETRY_DELAY)
                    continue
                return None, last_error

            return data, None

        except Exception as e:
            last_error = f"attempt {attempt}: {str(e)[:200]}"
            log.warning(f"  {last_error}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    return None, last_error


# ============================================================
# 回傳分析
# ============================================================

def analyze_response(
    api_name: str,
    city: str,
    model: str,
    target_date: str,
    data: dict,
    verbose: bool = False,
) -> dict:
    """
    分析 API 回傳資料，產出 daily 層級摘要。
    """
    summary = {
        "date": target_date,
        "city": city,
        "model": model,
        "source_api": api_name,
        "available": False,
        "hourly_count": 0,
        "daily_high_c": None,
        "daily_low_c": None,
        "vars_found": [],
        "vars_null_only": [],
        "vars_missing": [],
        "note": "",
    }

    if "hourly" not in data:
        summary["note"] = "no hourly block in response"
        return summary

    hourly = data["hourly"]
    times = hourly.get("time", [])
    summary["hourly_count"] = len(times)

    if verbose:
        log.info(f"  hourly.time 筆數: {len(times)}")
        if times:
            log.info(f"  時間範圍: {times[0]} ~ {times[-1]}")

    # 逐一檢查每個變數
    all_temps = []  # day0 溫度值

    for var in HOURLY_VARS:
        vals = hourly.get(var)
        if vals is None:
            summary["vars_missing"].append(var)
            if verbose:
                log.info(f"    {var}: 不存在")
            continue

        non_null = [v for v in vals if v is not None]

        if not non_null:
            summary["vars_null_only"].append(var)
            if verbose:
                log.info(f"    {var}: {len(vals)} 筆全是 null")
        else:
            summary["vars_found"].append(var)
            if verbose:
                log.info(f"    {var}: {len(non_null)}/{len(vals)} 筆有值, "
                         f"範圍 {min(non_null):.1f} ~ {max(non_null):.1f} °C")

            if var == "temperature_2m":
                all_temps = non_null

    # 從 day0 hourly 算 daily high/low
    if all_temps:
        summary["daily_high_c"] = round(max(all_temps), 2)
        summary["daily_low_c"] = round(min(all_temps), 2)
        summary["available"] = True

    # 組 note
    notes = []
    if summary["vars_missing"]:
        notes.append(f"{len(summary['vars_missing'])} vars missing")
    if summary["vars_null_only"]:
        notes.append(f"{len(summary['vars_null_only'])} vars all-null")
    if summary["vars_found"]:
        notes.append(f"{len(summary['vars_found'])} vars have data")
    summary["note"] = "; ".join(notes) if notes else "ok"

    return summary


# ============================================================
# 欄位檢查報告
# ============================================================

def print_field_report(summary: dict) -> None:
    """印出人類可讀的欄位檢查報告。"""
    tag = "OK" if summary["available"] else "FAIL"
    print(f"\n[{tag}] {summary['source_api']} — {summary['city']} × {summary['model']} × {summary['date']}")
    print(f"  hourly 筆數: {summary['hourly_count']}")
    print(f"  有資料的變數 ({len(summary['vars_found'])}): {summary['vars_found']}")

    if summary["vars_null_only"]:
        print(f"  全 null 變數 ({len(summary['vars_null_only'])}): {summary['vars_null_only']}")
    if summary["vars_missing"]:
        print(f"  不存在的變數 ({len(summary['vars_missing'])}): {summary['vars_missing']}")

    if summary["daily_high_c"] is not None:
        print(f"  daily high: {summary['daily_high_c']} °C")
        print(f"  daily low:  {summary['daily_low_c']} °C")
        print(f"  diurnal range: {round(summary['daily_high_c'] - summary['daily_low_c'], 2)} °C")

    print(f"  note: {summary['note']}")


# ============================================================
# 輸出
# ============================================================

def write_summary_csv(summaries: list[dict], csv_path: Path) -> None:
    """寫入探針摘要 CSV。"""
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for s in summaries:
            row = dict(s)
            # list 欄位轉成字串
            for k in ("vars_found", "vars_null_only", "vars_missing"):
                if isinstance(row.get(k), list):
                    row[k] = "|".join(row[k])
            writer.writerow(row)

    log.info(f"摘要 CSV 已寫入: {csv_path}")


def write_raw_json(raw_data: dict, json_path: Path) -> None:
    """寫入原始 JSON。"""
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=2)

    log.info(f"原始 JSON 已寫入: {json_path}")


# ============================================================
# 主流程
# ============================================================

def run(
    city: str,
    lat: float,
    lon: float,
    tz: str,
    model: str,
    target_date: str,
    api_name: str = DEFAULT_API,
    save_raw: bool = False,
    output_dir: Optional[Path] = None,
    verbose: bool = False,
) -> dict:
    """
    主流程：probe 一個城市 × 模型 × 日期 × endpoint。
    回傳 summary dict。
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    out_dir = output_dir or PROJ_DIR
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info("=" * 50)
    log.info("05_get_data_check: API 路線探針")
    log.info(f"  城市: {city} ({lat}, {lon}) tz={tz}")
    log.info(f"  模型: {model}")
    log.info(f"  日期: {target_date}")
    log.info(f"  API:  {api_name}")
    log.info("=" * 50)

    # 發 request
    data, error = probe_endpoint(
        api_name=api_name,
        lat=lat,
        lon=lon,
        tz=tz,
        model=model,
        target_date=target_date,
        verbose=verbose,
    )

    raw_output = {}

    if data:
        summary = analyze_response(
            api_name=api_name,
            city=city,
            model=model,
            target_date=target_date,
            data=data,
            verbose=verbose,
        )
        raw_output[api_name] = data
    else:
        summary = {
            "date": target_date,
            "city": city,
            "model": model,
            "source_api": api_name,
            "available": False,
            "hourly_count": 0,
            "daily_high_c": None,
            "daily_low_c": None,
            "vars_found": [],
            "vars_null_only": [],
            "vars_missing": [],
            "note": f"API error: {error}",
        }
        raw_output[api_name] = {"error": error}

    # 印出報告
    print_field_report(summary)

    # 寫 CSV
    csv_path = out_dir / DEFAULT_SUMMARY_CSV
    write_summary_csv([summary], csv_path)

    # 寫 raw JSON
    if save_raw:
        json_path = out_dir / DEFAULT_RAW_JSON
        write_raw_json(raw_output, json_path)

    log.info("完成。")
    return summary


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Open-Meteo API 路線探針：指定城市/模型/日期，檢查 API 是否可用",
    )
    p.add_argument("--city", type=str, required=True, help="城市名稱")
    p.add_argument("--lat", type=float, required=True, help="緯度")
    p.add_argument("--lon", type=float, required=True, help="經度")
    p.add_argument("--tz", type=str, required=True, help="時區（例如 Europe/London）")
    p.add_argument("--model", type=str, required=True,
                   help="模型名稱（例如 gfs_seamless）")
    p.add_argument("--date", type=str, required=True,
                   help="目標日期（YYYY-MM-DD）")
    p.add_argument("--api", type=str, default=DEFAULT_API,
                   choices=list(ENDPOINTS.keys()),
                   help=f"使用哪個 endpoint（預設: {DEFAULT_API}）")
    p.add_argument("--save-raw", action="store_true",
                   help="儲存原始 JSON 回傳")
    p.add_argument("--output-dir", type=str, default=None,
                   help="輸出目錄（預設: 同目錄）")
    p.add_argument("--verbose", action="store_true",
                   help="詳細 log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(
        city=args.city,
        lat=args.lat,
        lon=args.lon,
        tz=args.tz,
        model=args.model,
        target_date=args.date,
        api_name=args.api,
        save_raw=args.save_raw,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        verbose=args.verbose,
    )
