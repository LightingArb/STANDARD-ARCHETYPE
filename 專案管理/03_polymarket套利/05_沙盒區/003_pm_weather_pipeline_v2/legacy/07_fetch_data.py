"""
07_fetch_data.py — 正式資料抓取器

讀取 03_get_data_matrix.csv，依矩陣設定正式抓資料並落地存檔。

落地結構：
  {output_root}/{city}/{year}/{month:02d}/
    actual_iem.csv
    era5.csv
    forecast_{model}.csv

中斷 / 重跑策略：
  - 若檔案不存在 → 新抓
  - 若檔案已存在 → 補抓缺少的日期（upsert）
  - 近期 3 天的資料 → 強制重抓（rollback window）
  - 單一模型失敗不會讓整體 crash

用法：
  python 07_fetch_data.py
  python 07_fetch_data.py --cities London,Paris
  python 07_fetch_data.py --source-types D
  python 07_fetch_data.py --dry-run --verbose
"""

import argparse
import csv
import logging
import time
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

# ============================================================
# 常數
# ============================================================

PROJ_DIR = Path(__file__).resolve().parent

DEFAULT_MATRIX_CSV = PROJ_DIR / "logs" / "06_matrix" / "get_data_matrix.csv"
DEFAULT_OUTPUT_ROOT = PROJ_DIR / "data"

# --- API URLs ---

IEM_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
OM_HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
OM_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# --- D 來源 hourly 變數 ---

D_HOURLY_VARS = ["temperature_2m"]
for _i in range(1, 8):
    D_HOURLY_VARS.append(f"temperature_2m_previous_day{_i}")

# --- D 來源 multi-model request 分群 ---
# 同一群組的模型會合併成一次 API request。
# 不在任何群組中的模型，視為 singleton（單獨一次 request）。
D_MODEL_REQUEST_GROUPS = [
    ["gfs_seamless", "jma_seamless"],
]

# --- 網路設定 ---

A_REQUEST_TIMEOUT = 45
C_REQUEST_TIMEOUT = 45
D_REQUEST_TIMEOUT = 45
MAX_RETRIES = 3
RETRY_DELAY = 3      # 秒
API_DELAY = 0.1      # 每次 API call 間隔

# --- Rollback ---
# 距今 3 天內的資料，強制重抓（因為可能還沒穩定）
ROLLBACK_DAYS = 3
RECENT_COMPLETE_SKIP_SECONDS = 3600

# --- CSV 欄位定義 ---

A_CSV_FIELDS = [
    "date", "time_utc", "time_local", "temp_c", "source", "station", "fetch_time",
]

A_DAILY_CSV_FIELDS = [
    "date", "daily_high_c", "daily_low_c", "daily_range_c",
    "hourly_count", "source", "station", "fetch_time",
]

C_CSV_FIELDS = [
    "date", "daily_high_c", "daily_low_c", "daily_range_c",
    "hourly_count", "source", "fetch_time",
]

D_CSV_FIELDS = [
    "date", "model", "previous_day", "pred_daily_high_c", "pred_daily_low_c",
    "pred_diurnal_range_c", "hourly_count", "source", "fetch_time",
]

D_QUALITY_CSV_FIELDS = [
    "date", "model", "is_complete", "expected_hourly_count",
    "present_layer_count", "missing_layers", "min_non_null_count",
    "note", "fetch_time",
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
# 讀取矩陣
# ============================================================

def load_matrix(csv_path: Path) -> list[dict]:
    """讀取 03_get_data_matrix.csv。"""
    if not csv_path.exists():
        log.error(f"找不到矩陣: {csv_path}")
        return []

    rows = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))

    log.info(f"從矩陣讀到 {len(rows)} 筆")
    return rows


def filter_fetchable_rows(
    rows: list[dict],
    city_filter: Optional[list[str]] = None,
    source_filter: Optional[list[str]] = None,
) -> list[dict]:
    """
    篩選出可抓取的 rows：
      - available = True
      - fetch_enabled = true
      - fetch_start_month 不為空
    """
    result = []
    for row in rows:
        available = row.get("available", "").strip().lower() in ("true", "1")
        fetch_enabled = row.get("fetch_enabled", "").strip().lower() in ("true", "1", "yes")
        fetch_start = row.get("fetch_start_month", "").strip()

        if not available or not fetch_enabled or not fetch_start:
            continue

        if city_filter:
            if row.get("city", "").strip() not in city_filter:
                continue

        if source_filter:
            if row.get("source_type", "").strip() not in source_filter:
                continue

        result.append(row)

    log.info(f"可抓取的 rows: {len(result)}")
    return result


# ============================================================
# 月份範圍工具
# ============================================================

def parse_month(month_str: str) -> Optional[date]:
    """解析 'YYYY-MM' 為 date（取該月 1 號）。"""
    try:
        parts = month_str.strip().split("-")
        return date(int(parts[0]), int(parts[1]), 1)
    except Exception:
        return None


def generate_month_range(start_month: str, end_month: str) -> list[tuple[int, int]]:
    """
    產生月份列表 [(year, month), ...]。
    若 end_month 為空，抓到當前月份。
    """
    start = parse_month(start_month)
    if not start:
        return []

    if end_month and end_month.strip():
        end = parse_month(end_month)
        if not end:
            end = date.today().replace(day=1)
    else:
        end = date.today().replace(day=1)

    months = []
    current = start
    while current <= end:
        months.append((current.year, current.month))
        # 下個月
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


def get_month_date_range(year: int, month: int) -> tuple[date, date]:
    """取得某月的第一天和最後一天。"""
    first_day = date(year, month, 1)
    last_day_num = monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)
    return first_day, last_day


def clamp_month_end_for_current_month(start: date, end: date, tz: str = "UTC") -> tuple[date, date]:
    """
    若目標月份是當前月份，將 end clamp 到城市本地 today；
    否則維持原本的 (start, end)。
    """
    today = datetime.now(ZoneInfo(tz)).date()
    if start.year == today.year and start.month == today.month:
        return start, min(end, today)
    return start, end


def clamp_month_end_for_current_month_c_archive(start: date, end: date, tz: str = "UTC") -> tuple[date, date]:
    """
    C（Archive）專用 clamp：
    若目標月份是當前月份，將 end clamp 到城市本地 today-1；
    否則維持原本的 (start, end)。
    """
    today = datetime.now(ZoneInfo(tz)).date()
    if start.year == today.year and start.month == today.month:
        return start, min(end, today - timedelta(days=1))
    return start, end


# ============================================================
# 目錄結構
# ============================================================

def get_month_dir(output_root: Path, city: str, year: int, month: int) -> Path:
    """取得月份目錄路徑：{root}/{city}/{year}/{month:02d}/"""
    return output_root / city / str(year) / f"{month:02d}"


def ensure_dir(dir_path: Path) -> None:
    """確保目錄存在。"""
    dir_path.mkdir(parents=True, exist_ok=True)


# ============================================================
# CSV 讀寫工具（upsert）
# ============================================================

def read_existing_csv(csv_path: Path, key_fields: list[str]) -> dict[tuple, dict]:
    """
    讀取已存在的 CSV，回傳 {key_tuple: row_dict}。
    key_fields 用來組合唯一鍵。
    """
    if not csv_path.exists():
        return {}

    result = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = tuple(row.get(k, "") for k in key_fields)
            result[key] = dict(row)

    return result


def upsert_and_write(
    csv_path: Path,
    fieldnames: list[str],
    new_rows: list[dict],
    key_fields: list[str],
    rollback_dates: Optional[set[str]] = None,
) -> int:
    """
    讀取舊 CSV → upsert new_rows → 寫回。

    rollback_dates: 這些日期的舊資料會被強制覆蓋。
    回傳寫入的總列數。
    """
    existing = read_existing_csv(csv_path, key_fields)

    # 移除 rollback 日期的舊資料
    if rollback_dates:
        keys_to_remove = []
        for key, row in existing.items():
            row_date = row.get("date", "")
            if row_date in rollback_dates:
                keys_to_remove.append(key)
        for k in keys_to_remove:
            del existing[k]

    # Upsert 新資料
    for row in new_rows:
        key = tuple(row.get(k, "") for k in key_fields)
        existing[key] = row

    # 排序後寫入
    sorted_rows = sorted(existing.values(), key=lambda r: r.get("date", ""))

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted_rows:
            writer.writerow(row)

    return len(sorted_rows)


def write_full_csv(csv_path: Path, fieldnames: list[str], rows: list[dict]) -> int:
    """
    整檔覆蓋寫入 CSV。用於 D 來源的單月檔。
    若 rows 為空，仍寫出只有 header 的空檔��代表該月有跑過但無資料）。
    回傳寫入的資料���數。
    """
    sorted_rows = sorted(rows, key=lambda r: r.get("date", ""))
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted_rows:
            writer.writerow(row)
    return len(sorted_rows)


def compute_rollback_dates(tz: str = "UTC") -> set[str]:
    """計算需要 rollback 的日期（距城市本地今天 ROLLBACK_DAYS 天內）。"""
    today = datetime.now(ZoneInfo(tz)).date()
    dates = set()
    for i in range(ROLLBACK_DAYS):
        d = today - timedelta(days=i)
        dates.add(str(d))
    return dates


def enumerate_dates(start: date, end: date) -> set[str]:
    dates = set()
    current = start
    while current <= end:
        dates.add(str(current))
        current += timedelta(days=1)
    return dates


def parse_date_str(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def target_dates_to_range(target_dates: set[str]) -> tuple[date, date]:
    parsed = sorted(parse_date_str(value) for value in target_dates)
    return parsed[0], parsed[-1]


def get_effective_fetch_dates(source_type: str, year: int, month: int, tz: str = "UTC") -> set[str]:
    start, end = get_month_date_range(year, month)
    if source_type == "C":
        start, end = clamp_month_end_for_current_month_c_archive(start, end, tz=tz)
    else:
        start, end = clamp_month_end_for_current_month(start, end, tz=tz)
    if start > end:
        return set()
    return enumerate_dates(start, end)


def read_csv_dates(csv_path: Path) -> set[str]:
    if not csv_path.exists():
        return set()

    result = set()
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            row_date = row.get("date", "").strip()
            if row_date:
                result.add(row_date)
    return result


def read_quality_completion(csv_path: Path) -> dict[str, bool]:
    if not csv_path.exists():
        return {}

    result: dict[str, bool] = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            row_date = row.get("date", "").strip()
            if not row_date:
                continue
            result[row_date] = row.get("is_complete", "").strip().lower() in (
                "true", "1", "yes",
            )
    return result


def classify_month_cache(
    source_type: str,
    city: str,
    year: int,
    month: int,
    output_root: Path,
    rollback_dates: set[str],
    model: str = "",
) -> str:
    """
    Return one of:
      - skip_complete
      - refetch_due_to_rollback
      - fetch_new
    """
    month_dir = get_month_dir(output_root, city, year, month)
    expected_dates = get_effective_fetch_dates(source_type, year, month)
    stable_dates = expected_dates - rollback_dates

    if not stable_dates:
        return "fetch_new"

    has_rollback_overlap = bool(expected_dates & rollback_dates)
    recent_paths: list[Path] = []

    if source_type == "A":
        hourly_csv = month_dir / "actual_iem.csv"
        daily_csv = month_dir / "actual_iem_daily.csv"
        if not hourly_csv.exists() or not daily_csv.exists():
            return "fetch_new"
        recent_paths = [hourly_csv, daily_csv]

        hourly_dates = read_csv_dates(hourly_csv)
        daily_dates = read_csv_dates(daily_csv)
        is_complete = stable_dates.issubset(hourly_dates) and stable_dates.issubset(daily_dates)
    elif source_type == "C":
        era5_csv = month_dir / "era5.csv"
        if not era5_csv.exists():
            return "fetch_new"
        recent_paths = [era5_csv]

        daily_dates = read_csv_dates(era5_csv)
        is_complete = stable_dates.issubset(daily_dates)
    elif source_type == "D":
        forecast_csv = month_dir / f"forecast_{model}.csv"
        quality_csv = month_dir / f"forecast_{model}_quality.csv"
        if not forecast_csv.exists() or not quality_csv.exists():
            return "fetch_new"
        recent_paths = [forecast_csv, quality_csv]

        forecast_dates = read_csv_dates(forecast_csv)
        quality_map = read_quality_completion(quality_csv)
        is_complete = stable_dates.issubset(forecast_dates) and all(
            quality_map.get(day) is True for day in stable_dates
        )
    else:
        return "fetch_new"

    if not is_complete:
        return "fetch_new"
    if has_rollback_overlap:
        min_mtime = min(path.stat().st_mtime for path in recent_paths)
        if time.time() - min_mtime <= RECENT_COMPLETE_SKIP_SECONDS:
            return "skip_complete"
        return "refetch_due_to_rollback"
    return "skip_complete"


def classify_d_group_cache(
    city: str,
    models: list[str],
    year: int,
    month: int,
    output_root: Path,
    rollback_dates: set[str],
) -> str:
    actions = [
        classify_month_cache(
            source_type="D",
            city=city,
            year=year,
            month=month,
            output_root=output_root,
            rollback_dates=rollback_dates,
            model=model,
        )
        for model in models
    ]

    if actions and all(action == "skip_complete" for action in actions):
        return "skip_complete"
    if actions and all(action in ("skip_complete", "refetch_due_to_rollback") for action in actions):
        return "refetch_due_to_rollback"
    return "fetch_new"


# ============================================================
# A 來源：IEM
# ============================================================

def build_iem_request_params(station: str, start: date, end: date) -> dict:
    """組裝 IEM API 請求參數。"""
    return {
        "station": station,
        "data": "tmpf",
        "year1": start.year,
        "month1": start.month,
        "day1": start.day,
        "year2": end.year,
        "month2": end.month,
        "day2": end.day,
        "tz": "Etc/UTC",
        "format": "onlycomma",
        "latlon": "no",
        "elev": "no",
        "missing": "M",
        "trace": "T",
        "report_type": "3",
    }


def parse_iem_response(text: str, station: str, fetch_time: str, tz: str = "UTC") -> list[dict]:
    """
    解析 IEM CSV 回傳，轉成標準化 rows。

    IEM 回傳的 valid 欄位是 UTC 時間（因為 request 帶了 tz=Etc/UTC）。
    這裡先解析為 UTC datetime，再轉成城市本地時區，
    用本地日期作為 date，確保 daily 分組以城市本地日為準。
    """
    rows = []
    lines = text.strip().split("\n")

    if not lines or "tmpf" not in lines[0].lower():
        return rows

    header = [h.strip().lower() for h in lines[0].split(",")]
    valid_idx = header.index("valid") if "valid" in header else None
    tmpf_idx = header.index("tmpf") if "tmpf" in header else None

    if valid_idx is None or tmpf_idx is None:
        return rows

    # 準備時區物件
    utc_tz = ZoneInfo("UTC")
    try:
        local_tz = ZoneInfo(tz)
    except Exception:
        # tz 無效時 fallback UTC，此時 date 會是 UTC 日
        log.warning(f"  無法辨識時區 '{tz}'，A 來源將以 UTC 日處理")
        local_tz = utc_tz

    for line in lines[1:]:
        if not line.strip():
            continue

        parts = line.split(",")
        if len(parts) <= max(valid_idx, tmpf_idx):
            continue

        time_str = parts[valid_idx].strip()
        raw_tmpf = parts[tmpf_idx].strip()

        if raw_tmpf == "M" or raw_tmpf == "":
            continue

        try:
            temp_f = float(raw_tmpf)
            temp_c = round((temp_f - 32) * 5 / 9, 2)
        except ValueError:
            continue

        # UTC → 城市本地時區
        try:
            utc_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M").replace(tzinfo=utc_tz)
            local_dt = utc_dt.astimezone(local_tz)
            local_date = str(local_dt.date())
            local_time_str = local_dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            # 解析失敗時，退回用原始字串的前 10 碼當日期
            local_date = time_str[:10] if len(time_str) >= 10 else time_str
            local_time_str = ""

        rows.append({
            "date": local_date,           # 城市本地日期
            "time_utc": time_str,         # IEM 原始 UTC 時間
            "time_local": local_time_str, # 轉換後的本地時間
            "temp_c": temp_c,
            "source": "A_IEM",
            "station": station,
            "fetch_time": fetch_time,
        })

    return rows


def aggregate_iem_to_daily(raw_rows: list[dict], station: str, fetch_time: str) -> list[dict]:
    """
    將 IEM 逐筆原始觀測 rows 匯總為 daily 粒度。
    每天：max(temp_c) → daily_high_c, min(temp_c) → daily_low_c。
    """
    daily_map: dict[str, list[float]] = {}
    for row in raw_rows:
        d = row.get("date", "")
        try:
            val = float(row.get("temp_c", ""))
        except (ValueError, TypeError):
            continue
        if d not in daily_map:
            daily_map[d] = []
        daily_map[d].append(val)

    result = []
    for d in sorted(daily_map.keys()):
        vals = daily_map[d]
        high = round(max(vals), 2)
        low = round(min(vals), 2)
        result.append({
            "date": d,
            "daily_high_c": high,
            "daily_low_c": low,
            "daily_range_c": round(high - low, 2),
            "hourly_count": len(vals),
            "source": "A_IEM",
            "station": station,
            "fetch_time": fetch_time,
        })

    return result


def fetch_iem_monthly(
    city: str,
    station: str,
    tz: str,
    year: int,
    month: int,
    output_root: Path,
    target_dates: Optional[set[str]] = None,
    dry_run: bool = False,
) -> str:
    """
    抓取一個城市一個月份的 IEM 資料並落地。

    回傳狀態字串：
      "success"    — 成功抓取並落地
      "soft_fail"  — 上游暫時不可用（5xx / timeout / 連線失敗），非我方問題
      "hard_fail"  — 站點無效、請求格式錯誤、回傳空資料等確定性錯誤
    """
    import requests

    if target_dates:
        start, end = target_dates_to_range(target_dates)
        rewrite_dates = set(target_dates)
    else:
        start, end = get_month_date_range(year, month)
        start, end = clamp_month_end_for_current_month(start, end, tz=tz)
        rewrite_dates = compute_rollback_dates(tz=tz)
    # IEM 的 end_date 是 exclusive 的（要多加一天）
    end_plus = end + timedelta(days=1)
    fetch_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    month_dir = get_month_dir(output_root, city, year, month)
    csv_path = month_dir / "actual_iem.csv"

    if dry_run:
        log.info(f"  [DRY RUN] A: {city} {year}-{month:02d} → {csv_path}")
        return "success"

    ensure_dir(month_dir)

    params = build_iem_request_params(station, start, end_plus)
    last_error_type = "hard_fail"

    for attempt in range(1, MAX_RETRIES + 1):
        backoff = RETRY_DELAY * (2 ** (attempt - 1))  # 3s, 6s, 12s

        try:
            resp = requests.get(IEM_URL, params=params, timeout=A_REQUEST_TIMEOUT)

            # --- 5xx: 上游暫時不可用 ---
            if resp.status_code >= 500:
                log.warning(
                    f"  A IEM: upstream unavailable (HTTP {resp.status_code}) "
                    f"for {city}/{station} {year}-{month:02d} "
                    f"[attempt {attempt}/{MAX_RETRIES}]"
                )
                last_error_type = "soft_fail"
                if attempt < MAX_RETRIES:
                    log.info(f"  A IEM: retrying in {backoff}s...")
                    time.sleep(backoff)
                    continue
                return "soft_fail"

            # --- 404: 站點無效 ---
            if resp.status_code == 404:
                log.error(
                    f"  A IEM: station not found (HTTP 404) "
                    f"for {city}/{station} — check station_code validity"
                )
                return "hard_fail"

            # --- 其他 4xx: 請求格式錯誤 ---
            if resp.status_code >= 400:
                log.error(
                    f"  A IEM: request error (HTTP {resp.status_code}) "
                    f"for {city}/{station} {year}-{month:02d} — possible format issue"
                )
                return "hard_fail"

            # --- 200 OK ---
            rows = parse_iem_response(resp.text, station, fetch_time, tz=tz)
            if not rows:
                log.warning(f"  A IEM: {city}/{station} {year}-{month:02d} returned empty data")
                return "hard_fail"

            total = upsert_and_write(
                csv_path, A_CSV_FIELDS, rows,
                key_fields=["date", "time_utc"],
                rollback_dates=rewrite_dates,
            )
            log.info(f"  A IEM: {city} {year}-{month:02d} → {total} rows")

            # 額外產出 daily 匯總檔（用磁碟上合併後的完整 hourly 資料）
            daily_csv_path = month_dir / "actual_iem_daily.csv"
            all_hourly = read_existing_csv(csv_path, key_fields=["date", "time_utc"])
            daily_rows = aggregate_iem_to_daily(list(all_hourly.values()), station, fetch_time)
            if daily_rows:
                daily_total = upsert_and_write(
                    daily_csv_path, A_DAILY_CSV_FIELDS, daily_rows,
                    key_fields=["date"],
                    rollback_dates=rewrite_dates,
                )
                log.info(f"  A IEM daily: {city} {year}-{month:02d} → {daily_total} rows")

            return "success"

        except requests.exceptions.Timeout:
            log.warning(
                f"  A IEM: request timeout for {city}/{station} {year}-{month:02d} "
                f"[attempt {attempt}/{MAX_RETRIES}]"
            )
            last_error_type = "soft_fail"
            if attempt < MAX_RETRIES:
                log.info(f"  A IEM: retrying in {backoff}s...")
                time.sleep(backoff)

        except requests.exceptions.ConnectionError:
            log.warning(
                f"  A IEM: connection error for {city}/{station} {year}-{month:02d} "
                f"[attempt {attempt}/{MAX_RETRIES}]"
            )
            last_error_type = "soft_fail"
            if attempt < MAX_RETRIES:
                log.info(f"  A IEM: retrying in {backoff}s...")
                time.sleep(backoff)

        except Exception as e:
            log.error(
                f"  A IEM: unexpected error for {city}/{station} {year}-{month:02d}: {e}"
            )
            last_error_type = "hard_fail"
            if attempt < MAX_RETRIES:
                time.sleep(backoff)

    return last_error_type


# ============================================================
# C 來源：ERA5 / Open-Meteo Archive
# ============================================================

def build_archive_request_params(lat: float, lon: float, start: date, end: date, tz: str) -> dict:
    """
    組裝 Open-Meteo Archive API 請求參數。
    帶 timezone 確保 daily 分組以城市本地時區為準；
    若不帶，非 UTC 城市（如 Tokyo、Sydney）的日期邊界會錯位。
    """
    return {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "start_date": str(start),
        "end_date": str(end),
        "timezone": tz,
    }


def parse_archive_to_daily(data: dict, fetch_time: str) -> list[dict]:
    """
    從 Archive API 回傳的 hourly 資料，整理成 daily 粒度。
    每天 = max(hourly) → daily_high, min(hourly) → daily_low。
    """
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])

    if not times or not temps:
        return []

    # 按日期分組
    daily_map: dict[str, list[float]] = {}
    for i, t_str in enumerate(times):
        if i >= len(temps) or temps[i] is None:
            continue
        date_part = t_str[:10]
        if date_part not in daily_map:
            daily_map[date_part] = []
        daily_map[date_part].append(temps[i])

    rows = []
    for d in sorted(daily_map.keys()):
        vals = daily_map[d]
        high = round(max(vals), 2)
        low = round(min(vals), 2)
        rows.append({
            "date": d,
            "daily_high_c": high,
            "daily_low_c": low,
            "daily_range_c": round(high - low, 2),
            "hourly_count": len(vals),
            "source": "C_ERA5",
            "fetch_time": fetch_time,
        })

    return rows


def fetch_era5_monthly(
    city: str,
    lat: float,
    lon: float,
    tz: str,
    year: int,
    month: int,
    output_root: Path,
    target_dates: Optional[set[str]] = None,
    dry_run: bool = False,
) -> str:
    """
    抓取一個城市一個月份的 ERA5 / Archive 資料並落地。
    回傳 "success" / "soft_fail" / "hard_fail"。
    """
    import requests

    if target_dates:
        start, end = target_dates_to_range(target_dates)
        rewrite_dates = set(target_dates)
    else:
        start, end = get_month_date_range(year, month)
        start, end = clamp_month_end_for_current_month_c_archive(start, end, tz=tz)
        rewrite_dates = compute_rollback_dates(tz=tz)
    fetch_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    month_dir = get_month_dir(output_root, city, year, month)
    csv_path = month_dir / "era5.csv"

    if dry_run:
        log.info(f"  [DRY RUN] C: {city} {year}-{month:02d} → {csv_path}")
        return "success"

    ensure_dir(month_dir)

    params = build_archive_request_params(lat, lon, start, end, tz)
    last_error_type = "hard_fail"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(OM_ARCHIVE_URL, params=params, timeout=C_REQUEST_TIMEOUT)
            if resp.status_code >= 500:
                log.warning(f"  C ERA5 HTTP {resp.status_code} for {city} {year}-{month:02d} [attempt {attempt}/{MAX_RETRIES}]")
                last_error_type = "soft_fail"
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                return last_error_type
            if resp.status_code != 200:
                log.warning(f"  C ERA5 HTTP {resp.status_code} for {city} {year}-{month:02d}")
                return "hard_fail"

            data = resp.json()
            rows = parse_archive_to_daily(data, fetch_time)

            if not rows:
                log.warning(f"  C ERA5: {city} {year}-{month:02d} 無資料")
                return "hard_fail"

            total = upsert_and_write(
                csv_path, C_CSV_FIELDS, rows,
                key_fields=["date"],
                rollback_dates=rewrite_dates,
            )
            log.info(f"  C ERA5: {city} {year}-{month:02d} → {total} rows")
            return "success"

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            log.warning(f"  C ERA5 {type(e).__name__} ({city} {year}-{month:02d}, attempt {attempt}/{MAX_RETRIES}): {e}")
            last_error_type = "soft_fail"
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

        except Exception as e:
            log.warning(f"  C ERA5 error ({city} {year}-{month:02d}, attempt {attempt}): {e}")
            last_error_type = "hard_fail"
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    return last_error_type


# ============================================================
# D 來源：Open-Meteo Historical Forecast
# ============================================================

def get_model_request_group(model: str) -> list[str]:
    """回傳模型所屬的 request group。若不在任何群組中，回傳 [model]。"""
    for group in D_MODEL_REQUEST_GROUPS:
        if model in group:
            return list(group)
    return [model]


def build_effective_groups(fetchable_rows: list[dict]) -> dict:
    """
    根據 fetchable_rows 預先計算 D 模型在各城市 / 月份的 effective group。

    key:   (city, year, month, model)
    value: list[str]
    """
    active_models_by_month: dict[tuple[str, int, int], set[str]] = {}
    d_rows_with_months: list[tuple[dict, list[tuple[int, int]]]] = []

    for row in fetchable_rows:
        if row.get("source_type", "").strip() != "D":
            continue

        city = row.get("city", "").strip()
        model = row.get("model", "").strip()
        fetch_start = row.get("fetch_start_month", "").strip()
        fetch_end = row.get("fetch_end_month", "").strip()

        if not city or not model:
            log.warning(f"build_effective_groups: D row 缺 city/model，略過預計算: {row}")
            continue

        months = generate_month_range(fetch_start, fetch_end)
        if not months:
            log.warning(f"build_effective_groups: 無效月份範圍，略過預計算: {city} {model} "
                        f"start={fetch_start} end={fetch_end}")
            continue

        d_rows_with_months.append((row, months))
        for year, month in months:
            month_key = (city, year, month)
            if month_key not in active_models_by_month:
                active_models_by_month[month_key] = set()
            active_models_by_month[month_key].add(model)

    effective_groups = {}
    for row, months in d_rows_with_months:
        city = row["city"].strip()
        model = row["model"].strip()

        static_group = get_model_request_group(model)
        in_static_group = any(model in group for group in D_MODEL_REQUEST_GROUPS)

        for year, month in months:
            if in_static_group:
                active_models = active_models_by_month.get((city, year, month), set())
                effective_group = [m for m in static_group if m in active_models]
                if not effective_group:
                    effective_group = [model]
            else:
                effective_group = [model]

            effective_groups[(city, year, month, model)] = effective_group

    return effective_groups


def extract_single_model_hourly(hourly: dict, model: str) -> dict:
    """
    從 multi-model response 的 hourly dict 中，提取單一模型的資料。

    multi-model key 格式：{base_var}_{model_name}
    例如：temperature_2m_gfs_seamless, temperature_2m_previous_day1_gfs_seamless

    回傳格式與 single-model response 一致：
    {time: [...], temperature_2m: [...], temperature_2m_previous_day1: [...], ...}
    """
    result = {"time": hourly.get("time", [])}
    for var in D_HOURLY_VARS:
        suffixed_key = f"{var}_{model}"
        result[var] = hourly.get(suffixed_key, [])
    return result


def build_forecast_request_params(
    lat: float, lon: float, models: str, start: date, end: date, tz: str,
) -> dict:
    """
    組裝 Open-Meteo Historical Forecast API 請求參數。
    models 為逗號分隔的模型名稱字串，支援 multi-model request。
    帶 timezone 確保 daily 分組以城市本地時區為準；
    若不帶，非 UTC 城市的日期邊界會錯位。
    """
    return {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(D_HOURLY_VARS),
        "start_date": str(start),
        "end_date": str(end),
        "models": models,
        "timezone": tz,
    }


def parse_forecast_to_daily(data: dict, model: str, fetch_time: str) -> list[dict]:
    """
    從 Historical Forecast API 回傳的 hourly 資料，整理成 daily 粒度。

    每天 × 每個 previous_day 層級：
      - pred_daily_high_c = max(24 hourly)
      - pred_daily_low_c  = min(24 hourly)
      - pred_diurnal_range_c = high - low
    """
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])

    if not times:
        return []

    rows = []

    for var in D_HOURLY_VARS:
        vals = hourly.get(var, [])
        if not vals:
            continue

        # 判斷 previous_day 層級
        if var == "temperature_2m":
            prev_day = 0
        else:
            prev_day = int(var.split("_previous_day")[1])

        # 按日期分組
        daily_map: dict[str, list[float]] = {}
        for i, t_str in enumerate(times):
            if i >= len(vals) or vals[i] is None:
                continue
            date_part = t_str[:10]
            if date_part not in daily_map:
                daily_map[date_part] = []
            daily_map[date_part].append(vals[i])

        for d in sorted(daily_map.keys()):
            day_vals = daily_map[d]
            high = round(max(day_vals), 2)
            low = round(min(day_vals), 2)
            rows.append({
                "date": d,
                "model": model,
                "previous_day": prev_day,
                "pred_daily_high_c": high,
                "pred_daily_low_c": low,
                "pred_diurnal_range_c": round(high - low, 2),
                "hourly_count": len(day_vals),
                "source": "D_openmeteo",
                "fetch_time": fetch_time,
            })

    return rows


def build_daily_completeness_map(hourly: dict) -> dict[str, dict]:
    """
    對 hourly 資料中的每一天，檢查 D 來源 8 層是否都完整。

    判定規則與 03 一致：
    1. D_HOURLY_VARS 裡 8 個變數都必須存在
    2. 每個變數在該日的 non-null 筆數 = 該日 timestamp 數量
       （不硬寫死 24，用 hourly.time 實際算）

    回傳 {date: {is_complete, expected_hourly_count, present_layer_count,
                  missing_layers, min_non_null_count, note}}
    """
    times = hourly.get("time", [])
    if not times:
        return {}

    # 按日期分組 timestamp index
    date_indices: dict[str, list[int]] = {}
    for i, t in enumerate(times):
        d = t[:10]
        if d not in date_indices:
            date_indices[d] = []
        date_indices[d].append(i)

    result = {}
    for d in sorted(date_indices.keys()):
        indices = date_indices[d]
        expected = len(indices)
        missing_layers = []
        incomplete_layers = []
        min_non_null = expected
        present_count = 0

        for var in D_HOURLY_VARS:
            vals = hourly.get(var)
            if vals is None or len(vals) == 0:
                short = var.replace("temperature_2m_", "") if "previous" in var else "day0"
                missing_layers.append(short)
                min_non_null = 0
                continue

            present_count += 1
            non_null = sum(1 for i in indices if i < len(vals) and vals[i] is not None)
            min_non_null = min(min_non_null, non_null)
            if non_null != expected:
                short = var.replace("temperature_2m_", "") if "previous" in var else "day0"
                incomplete_layers.append(short)

        is_complete = not missing_layers and not incomplete_layers

        if missing_layers:
            note = f"missing_vars={','.join(missing_layers)}"
        elif incomplete_layers:
            note = f"incomplete_hourly: {','.join(incomplete_layers)}"
        else:
            note = "complete"

        result[d] = {
            "is_complete": is_complete,
            "expected_hourly_count": expected,
            "present_layer_count": present_count,
            "missing_layers": ",".join(missing_layers),
            "min_non_null_count": min_non_null,
            "note": note,
        }

    return result


def fetch_forecast_monthly(
    city: str,
    lat: float,
    lon: float,
    tz: str,
    models: list[str],
    year: int,
    month: int,
    output_root: Path,
    target_dates: Optional[set[str]] = None,
    dry_run: bool = False,
) -> str:
    """
    抓取一個城市 × 一組模型 × 一個月份的 Historical Forecast 資料並落地。
    支援 multi-model request：一次 request 抓多個模型，拆解後逐模型寫檔。
    檔名仍為 per-model：forecast_{model}.csv / forecast_{model}_quality.csv
    回傳 "success" / "soft_fail" / "hard_fail"。
    """
    import requests

    is_multimodel = len(models) > 1
    models_str = ",".join(models)

    if target_dates:
        start, end = target_dates_to_range(target_dates)
        rewrite_dates = set(target_dates)
    else:
        start, end = get_month_date_range(year, month)
        start, end = clamp_month_end_for_current_month(start, end, tz=tz)
        rewrite_dates = compute_rollback_dates(tz=tz)
    fetch_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    month_dir = get_month_dir(output_root, city, year, month)

    if dry_run:
        log.info(f"  [DRY RUN] D: {city} × [{models_str}] {year}-{month:02d}")
        return "success"

    ensure_dir(month_dir)

    params = build_forecast_request_params(lat, lon, models_str, start, end, tz)
    last_error_type = "hard_fail"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                OM_HISTORICAL_FORECAST_URL,
                params=params,
                timeout=D_REQUEST_TIMEOUT,
            )

            if resp.status_code != 200:
                resp_data = {}
                try:
                    resp_data = resp.json()
                except Exception:
                    pass
                reason = resp_data.get("reason", f"HTTP {resp.status_code}")
                log.warning(f"  D [{models_str}] HTTP {resp.status_code} for {city} {year}-{month:02d}: {reason}")
                if resp.status_code >= 500:
                    last_error_type = "soft_fail"
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                        continue
                    return last_error_type
                return "hard_fail"

            data = resp.json()
            raw_hourly = data.get("hourly", {})

            # 逐模型拆解並寫檔
            for model in models:
                # 取得該模型的 hourly block（標準 key 格式）
                if is_multimodel:
                    model_hourly = extract_single_model_hourly(raw_hourly, model)
                else:
                    model_hourly = raw_hourly

                # 逐日品質檢查：8 層是否完整
                completeness = build_daily_completeness_map(model_hourly)
                complete_dates = {d for d, info in completeness.items() if info["is_complete"]}

                n_total = len(completeness)
                n_complete = len(complete_dates)
                n_incomplete = n_total - n_complete
                log.info(f"  D {model}: {city} {year}-{month:02d} "
                         f"品質: {n_complete} complete / {n_incomplete} incomplete (共 {n_total} 天)")

                # 品質檢查檔：整月覆蓋寫入（不 upsert，避免舊狀態殘留）
                quality_csv_path = month_dir / f"forecast_{model}_quality.csv"
                quality_rows = []
                for d in sorted(completeness.keys()):
                    info = completeness[d]
                    quality_rows.append({
                        "date": d,
                        "model": model,
                        "is_complete": str(info["is_complete"]),
                        "expected_hourly_count": info["expected_hourly_count"],
                        "present_layer_count": info["present_layer_count"],
                        "missing_layers": info["missing_layers"],
                        "min_non_null_count": info["min_non_null_count"],
                        "note": info["note"],
                        "fetch_time": fetch_time,
                    })

                if target_dates:
                    upsert_and_write(
                        quality_csv_path,
                        D_QUALITY_CSV_FIELDS,
                        quality_rows,
                        key_fields=["date", "model"],
                        rollback_dates=rewrite_dates,
                    )
                else:
                    write_full_csv(quality_csv_path, D_QUALITY_CSV_FIELDS, quality_rows)

                # 主資料檔：整月覆蓋寫入（只保留 complete 日期，舊 incomplete 不會殘留）
                model_data = {"hourly": model_hourly}
                all_daily_rows = parse_forecast_to_daily(model_data, model, fetch_time)
                complete_rows = [r for r in all_daily_rows if r["date"] in complete_dates]

                csv_path = month_dir / f"forecast_{model}.csv"
                if target_dates:
                    total = upsert_and_write(
                        csv_path,
                        D_CSV_FIELDS,
                        complete_rows,
                        key_fields=["date", "model", "previous_day"],
                        rollback_dates=rewrite_dates,
                    )
                else:
                    total = write_full_csv(csv_path, D_CSV_FIELDS, complete_rows)

                if total > 0:
                    log.info(f"  D {model}: {city} {year}-{month:02d} → {total} rows (complete only)")
                elif n_total > 0:
                    log.info(f"  D {model}: {city} {year}-{month:02d} 全月無完整日期，主檔已清空（僅 header）")
                else:
                    log.warning(f"  D {model}: {city} {year}-{month:02d} 無資料")

            return "success"

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            log.warning(f"  D [{models_str}] {type(e).__name__} ({city} {year}-{month:02d}, attempt {attempt}/{MAX_RETRIES}): {e}")
            last_error_type = "soft_fail"
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

        except Exception as e:
            log.warning(f"  D [{models_str}] error ({city} {year}-{month:02d}, attempt {attempt}): {e}")
            last_error_type = "hard_fail"
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    return last_error_type


# ============================================================
# 城市 metadata 查找
# ============================================================

def load_city_metadata(city_csv_path: Path) -> dict[str, dict]:
    """
    從 01_city.csv 讀取城市 metadata（lat, lon, station_code, timezone）。
    回傳 {city_name: {lat, lon, station_code, timezone}}。
    """
    if not city_csv_path.exists():
        return {}

    result = {}
    with open(city_csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row.get("city", "").strip()
            if city:
                result[city] = {
                    "lat": row.get("lat", "").strip(),
                    "lon": row.get("lon", "").strip(),
                    "station_code": row.get("station_code", "").strip(),
                    "timezone": row.get("timezone", "").strip(),
                }

    return result


# ============================================================
# 主流程
# ============================================================

def run(
    matrix_path: Optional[Path] = None,
    output_root: Optional[Path] = None,
    city_filter: Optional[list[str]] = None,
    source_filter: Optional[list[str]] = None,
    force_refetch: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """
    主流程：讀矩陣 → 按城市/來源/模型/月份逐一抓取 → 落地。
    回傳 {success: int, fail: int, skip: int, grouped_skip: int}。
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    matrix_csv = matrix_path or DEFAULT_MATRIX_CSV
    out_root = output_root or DEFAULT_OUTPUT_ROOT

    # 城市 metadata
    city_csv_path = PROJ_DIR / "data" / "01_city.csv"
    city_meta = load_city_metadata(city_csv_path)

    log.info("=" * 50)
    log.info("07_fetch_data: 正式資料抓取")
    log.info(f"  矩陣: {matrix_csv}")
    log.info(f"  輸出根目錄: {out_root}")
    log.info(f"  force_refetch: {force_refetch}")
    log.info("=" * 50)

    # Step 1: 讀矩陣
    all_rows = load_matrix(matrix_csv)
    if not all_rows:
        log.error("矩陣為空")
        return {
            "success": 0,
            "fail": 0,
            "skip": 0,
            "grouped_skip": 0,
            "soft_fail": 0,
            "skipped_complete": 0,
            "refetched_due_to_rollback": 0,
            "forced_refetch": 0,
            "fetched_new": 0,
        }

    # Step 2: 篩選可抓取 rows
    fetchable = filter_fetchable_rows(all_rows, city_filter, source_filter)
    if not fetchable:
        log.warning("沒有可抓取的 rows")
        return {
            "success": 0,
            "fail": 0,
            "skip": 0,
            "grouped_skip": 0,
            "soft_fail": 0,
            "skipped_complete": 0,
            "refetched_due_to_rollback": 0,
            "forced_refetch": 0,
            "fetched_new": 0,
        }

    effective_groups = build_effective_groups(fetchable)
    _rollback_cache: dict[str, set[str]] = {}  # tz -> rollback_dates

    # Step 3: 逐一處理
    stats = {
        "success": 0,
        "fail": 0,
        "skip": 0,
        "grouped_skip": 0,
        "soft_fail": 0,
        "skipped_complete": 0,
        "refetched_due_to_rollback": 0,
        "forced_refetch": 0,
        "fetched_new": 0,
    }

    # D 來源 group 追蹤：避免同一城市 × 同一月份對同 effective group 重複 request
    # key = (city, year, month), value = set of frozenset(effective_group)
    d_handled_groups: dict[tuple, set] = {}

    for row in fetchable:
        city = row["city"].strip()
        source_type = row["source_type"].strip()
        model = row.get("model", "").strip()
        fetch_start = row["fetch_start_month"].strip()
        fetch_end = row.get("fetch_end_month", "").strip()

        # 取 metadata
        meta = city_meta.get(city, {})
        lat_str = meta.get("lat", "")
        lon_str = meta.get("lon", "")
        station = meta.get("station_code", "")
        tz = meta.get("timezone", "") or "UTC"

        # 按城市時區計算 rollback dates（快取避免重複計算）
        if tz not in _rollback_cache:
            _rollback_cache[tz] = compute_rollback_dates(tz=tz)
        rollback_dates = _rollback_cache[tz]

        # 產生月份範圍
        months = generate_month_range(fetch_start, fetch_end)
        if not months:
            log.warning(f"  無效的月份範圍: {city} {source_type} {model} "
                        f"start={fetch_start} end={fetch_end}")
            stats["skip"] += 1
            continue

        log.info(f"抓取: {city} × {source_type}"
                 + (f" × {model}" if model else "")
                 + f" ({len(months)} 個月份)")

        for year, month in months:
            ok = False
            rollback_target_dates: set[str] | None = None

            try:
                if source_type == "A":
                    if not station:
                        log.warning(f"  A: {city} 無 station，跳過")
                        stats["skip"] += 1
                        continue

                    cache_action = "forced_refetch" if force_refetch else classify_month_cache(
                        source_type="A",
                        city=city,
                        year=year,
                        month=month,
                        output_root=out_root,
                        rollback_dates=rollback_dates,
                    )
                    if cache_action == "skip_complete":
                        log.info(
                            f"  A: {city} {year}-{month:02d} 已完整覆蓋（排除 rollback window），skip"
                        )
                        stats["skipped_complete"] += 1
                        continue
                    if cache_action == "forced_refetch":
                        log.info(f"  A: {city} {year}-{month:02d} force-refetch")
                        stats["forced_refetch"] += 1
                    elif cache_action == "refetch_due_to_rollback":
                        log.info(f"  A: {city} {year}-{month:02d} 因 rollback window 重抓")
                        stats["refetched_due_to_rollback"] += 1
                        rollback_target_dates = get_effective_fetch_dates("A", year, month, tz=tz) & rollback_dates
                    else:
                        stats["fetched_new"] += 1

                    iem_result = fetch_iem_monthly(
                        city=city,
                        station=station,
                        tz=tz,
                        year=year,
                        month=month,
                        output_root=out_root,
                        target_dates=rollback_target_dates,
                        dry_run=dry_run,
                    )
                    if iem_result == "success":
                        stats["success"] += 1
                    elif iem_result == "soft_fail":
                        stats["soft_fail"] += 1
                    else:
                        stats["fail"] += 1
                    time.sleep(API_DELAY)
                    continue

                elif source_type == "C":
                    if not lat_str or not lon_str:
                        log.warning(f"  C: {city} 無 lat/lon，跳過")
                        stats["skip"] += 1
                        continue

                    cache_action = "forced_refetch" if force_refetch else classify_month_cache(
                        source_type="C",
                        city=city,
                        year=year,
                        month=month,
                        output_root=out_root,
                        rollback_dates=rollback_dates,
                    )
                    if cache_action == "skip_complete":
                        log.info(
                            f"  C: {city} {year}-{month:02d} 已完整覆蓋（排除 rollback window），skip"
                        )
                        stats["skipped_complete"] += 1
                        continue
                    if cache_action == "forced_refetch":
                        log.info(f"  C: {city} {year}-{month:02d} force-refetch")
                        stats["forced_refetch"] += 1
                    elif cache_action == "refetch_due_to_rollback":
                        log.info(f"  C: {city} {year}-{month:02d} 因 rollback window 重抓")
                        stats["refetched_due_to_rollback"] += 1
                        rollback_target_dates = get_effective_fetch_dates("C", year, month, tz=tz) & rollback_dates
                    else:
                        stats["fetched_new"] += 1

                    era5_result = fetch_era5_monthly(
                        city=city,
                        lat=float(lat_str),
                        lon=float(lon_str),
                        tz=tz,
                        year=year,
                        month=month,
                        output_root=out_root,
                        target_dates=rollback_target_dates,
                        dry_run=dry_run,
                    )
                    if era5_result == "success":
                        stats["success"] += 1
                    elif era5_result == "soft_fail":
                        stats["soft_fail"] += 1
                    else:
                        stats["fail"] += 1
                    time.sleep(API_DELAY)
                    continue

                elif source_type == "D":
                    if not lat_str or not lon_str:
                        log.warning(f"  D: {city} 無 lat/lon，跳過")
                        stats["skip"] += 1
                        continue

                    if not model:
                        log.warning(f"  D: {city} 無 model，跳過")
                        stats["skip"] += 1
                        continue

                    effective_group = effective_groups.get((city, year, month, model))
                    if effective_group is None:
                        log.warning(f"  D {model}: {city} {year}-{month:02d} 查無 effective group，視為失敗")
                        stats["fail"] += 1
                        continue

                    group_key = frozenset(effective_group)
                    fetch_key = (city, year, month)

                    if fetch_key not in d_handled_groups:
                        d_handled_groups[fetch_key] = set()

                    if group_key in d_handled_groups[fetch_key]:
                        log.info(f"  D {model}: {city} {year}-{month:02d} 已由同 effective group request 處理，grouped_skip")
                        stats["grouped_skip"] += 1
                        continue

                    d_handled_groups[fetch_key].add(group_key)

                    cache_action = "forced_refetch" if force_refetch else classify_d_group_cache(
                        city=city,
                        models=effective_group,
                        year=year,
                        month=month,
                        output_root=out_root,
                        rollback_dates=rollback_dates,
                    )
                    if cache_action == "skip_complete":
                        log.info(
                            f"  D [{','.join(effective_group)}]: {city} {year}-{month:02d} 已完整覆蓋（排除 rollback window），skip"
                        )
                        stats["skipped_complete"] += 1
                        continue
                    if cache_action == "forced_refetch":
                        log.info(f"  D [{','.join(effective_group)}]: {city} {year}-{month:02d} force-refetch")
                        stats["forced_refetch"] += 1
                    elif cache_action == "refetch_due_to_rollback":
                        log.info(f"  D [{','.join(effective_group)}]: {city} {year}-{month:02d} 因 rollback window 重抓")
                        stats["refetched_due_to_rollback"] += 1
                        rollback_target_dates = get_effective_fetch_dates("D", year, month, tz=tz) & rollback_dates
                    else:
                        stats["fetched_new"] += 1

                    d_result = fetch_forecast_monthly(
                        city=city,
                        lat=float(lat_str),
                        lon=float(lon_str),
                        tz=tz,
                        models=effective_group,
                        year=year,
                        month=month,
                        output_root=out_root,
                        target_dates=rollback_target_dates,
                        dry_run=dry_run,
                    )
                    if d_result == "success":
                        stats["success"] += 1
                    elif d_result == "soft_fail":
                        stats["soft_fail"] += 1
                    else:
                        stats["fail"] += 1
                    time.sleep(API_DELAY)
                    continue

                else:
                    log.warning(f"  未知的 source_type: {source_type}，跳過")
                    stats["skip"] += 1
                    continue

            except Exception as e:
                log.error(f"  未預期錯誤: {city} {source_type} {model} "
                          f"{year}-{month:02d}: {e}")
                stats["fail"] += 1

            time.sleep(API_DELAY)

    # 報告
    log.info("=" * 50)
    log.info("抓取完成")
    log.info(f"  成功: {stats['success']}")
    log.info(f"  失敗: {stats['fail']}")
    log.info(f"  軟失敗 (上游暫時不可用): {stats['soft_fail']}")
    log.info(f"  fetched_new: {stats['fetched_new']}")
    log.info(f"  refetched_due_to_rollback: {stats['refetched_due_to_rollback']}")
    log.info(f"  forced_refetch: {stats['forced_refetch']}")
    log.info(f"  skipped_complete: {stats['skipped_complete']}")
    log.info(f"  跳過: {stats['skip']}")
    log.info(f"  grouped_skip: {stats['grouped_skip']}")
    log.info("=" * 50)

    return stats


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="讀取矩陣，依設定正式抓取天氣資料並落地存檔",
    )
    p.add_argument("--matrix", type=str, default=None,
                   help="矩陣 CSV 路徑（預設: 同目錄 03_get_data_matrix.csv）")
    p.add_argument("--cities", type=str, default=None,
                   help="只處理指定城市（逗號分隔）")
    p.add_argument("--source-types", type=str, default=None,
                   help="只處理指定來源（逗號分隔，例如 A,C,D）")
    p.add_argument("--output-root", type=str, default=None,
                   help="輸出根目錄（預設: 同目錄 data/）")
    p.add_argument("--force-refetch", action="store_true",
                   help="忽略完整月份檢查，強制重抓")
    p.add_argument("--dry-run", action="store_true",
                   help="只印出計畫，不實際抓取")
    p.add_argument("--verbose", action="store_true",
                   help="詳細 log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()

    city_filter = None
    if args.cities:
        city_filter = [c.strip() for c in args.cities.split(",") if c.strip()]

    source_filter = None
    if args.source_types:
        source_filter = [s.strip() for s in args.source_types.split(",") if s.strip()]

    run(
        matrix_path=Path(args.matrix) if args.matrix else None,
        output_root=Path(args.output_root) if args.output_root else None,
        city_filter=city_filter,
        source_filter=source_filter,
        force_refetch=args.force_refetch,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
