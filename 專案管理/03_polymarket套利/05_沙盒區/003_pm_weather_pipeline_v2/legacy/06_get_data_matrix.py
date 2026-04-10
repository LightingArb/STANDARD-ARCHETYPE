"""
06_get_data_matrix.py — 資料可用性矩陣產生器

讀取 01_city.csv，對每個城市 × 每個資料來源 × 每個模型，
探測最遠能抓到哪個月份，產出 03_get_data_matrix.csv。

這張表分兩類欄位：
  - 事實欄位（系統探測結果）：available, earliest_month, probe_rule 等
  - 決策欄位（人工抓取策略）：fetch_enabled, fetch_start_month, fetch_end_month

重新生成時，事實欄位會重算，但盡量保留舊表中的決策欄位。

用法：
  python 06_get_data_matrix.py
  python 06_get_data_matrix.py --cities London,Paris
  python 06_get_data_matrix.py --current-year 2026
  python 06_get_data_matrix.py --dry-run --verbose
"""

import argparse
import csv
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from _lib import resolve_active_d_models

# ============================================================
# 常數
# ============================================================

PROJ_DIR = Path(__file__).resolve().parent

DEFAULT_CITY_CSV = PROJ_DIR / "data" / "01_city.csv"
DEFAULT_MATRIX_CSV = PROJ_DIR / "logs" / "06_matrix" / "get_data_matrix.csv"

# --- 資料來源定義 ---

SOURCE_A = "A"   # IEM actual（METAR 觀測）
SOURCE_C = "C"   # ERA5 / Historical Weather（Open-Meteo Archive API）
SOURCE_D = "D"   # Open-Meteo 19 個 forecast models

# --- D 的 19 個模型 ---
# 保留完整模型清單；實際 monthly 預設 active set 會再套用
# resolve_active_d_models()，先排除已知停用的 D13 / D14 / D18。

D_MODELS = [
    "gfs_seamless",                     # D1
    "jma_seamless",                     # D2
    "jma_gsm",                          # D3
    "knmi_seamless",                    # D4
    "dmi_seamless",                     # D5
    "metno_seamless",                   # D6
    "best_match",                       # D7
    "gfs_global",                       # D8
    "ukmo_seamless",                    # D9
    "ukmo_global_deterministic_10km",   # D10
    "icon_seamless",                    # D11
    "icon_global",                      # D12
    "icon_eu",                          # D13
    "icon_d2",                          # D14
    "gem_seamless",                     # D15
    "gem_global",                       # D16
    "meteofrance_seamless",             # D17
    "meteofrance_arpege",               # D18
    "cma_grapes_global",                # D19
]

# --- D 來源必須���整的 8 個 hourly 變��� ---
# 某天若要算 available，這 8 層都必須完整（non-null count = expected hourly count）

D_REQUIRED_VARS = [
    "temperature_2m",
    "temperature_2m_previous_day1",
    "temperature_2m_previous_day2",
    "temperature_2m_previous_day3",
    "temperature_2m_previous_day4",
    "temperature_2m_previous_day5",
    "temperature_2m_previous_day6",
    "temperature_2m_previous_day7",
]

# --- Open-Meteo API ---

OM_HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
OM_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# IEM
IEM_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

# --- A / C 的靜態規則 ---
# IEM 資料非常長（多數站點 2000 年以前就有），用保守常數
A_STATIC_EARLIEST = "2000-01"

# ERA5 / Open-Meteo Archive 資料從 1940 年起，我們保守設 1980 年
C_STATIC_EARLIEST = "1980-01"

# --- 網路設定 ---
REQUEST_TIMEOUT = 20
PROBE_DELAY = 0.15  # 每次 probe 間隔秒數

# --- Probe cache / resume 設定 ---

PROBE_VERSION = "matrix_probe_v2_resume_v1"
PROBE_STATUS_PENDING = "pending"
PROBE_STATUS_RUNNING = "running"
PROBE_STATUS_DONE = "done"
PROBE_STATUS_FAILED = "failed"
PROBE_STATUS_SKIPPED = "skipped"
PROBE_RUNNING_STALE_SECONDS = 3600
EXPLICIT_AVAILABLE_VALUES = {"true", "false"}

# --- 矩陣 CSV 欄位 ---

MATRIX_FIELDS = [
    # 事實欄位
    "city",
    "source_type",
    "model",
    "available",
    "earliest_month",
    "latest_probe_time",
    "probe_rule",
    "note",
    "probe_status",
    "probe_completed_at",
    "probe_version",
    "probe_basis_year",
    "probe_attempts",
    # 決策欄位
    "fetch_enabled",
    "fetch_start_month",
    "fetch_end_month",
]

ROW_KEY_FIELDS = ("city", "source_type", "model")

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ============================================================
# 讀取 01_city.csv
# ============================================================

def load_city_csv(csv_path: Path) -> list[dict]:
    """讀取 01_city.csv，回傳 list[dict]。"""
    if not csv_path.exists():
        log.error(f"找不到 {csv_path}")
        return []

    rows = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))

    log.info(f"從 {csv_path.name} 讀到 {len(rows)} 個城市")
    return rows


def filter_enabled_cities(cities: list[dict], city_filter: Optional[list[str]] = None) -> list[dict]:
    """
    篩選出 metadata 完整且 city_enabled=true 的城市。
    若 city_filter 有值，再做城市名過濾。
    """
    result = []
    for c in cities:
        city_name = c.get("city", "").strip()

        if city_filter and city_name not in city_filter:
            continue

        # 檢查 city_enabled 和 metadata 完整性
        meta_ok, note = check_city_metadata(c)
        if not meta_ok:
            log.info(f"  跳過 {city_name}: {note}")
            continue

        result.append(c)

    return result


def check_city_metadata(city: dict) -> tuple[bool, str]:
    """
    檢查城市 metadata 是否完整。
    回傳 (is_complete, note_if_incomplete)。
    """
    missing = []
    if not city.get("lat", "").strip():
        missing.append("lat")
    if not city.get("lon", "").strip():
        missing.append("lon")
    if not city.get("timezone", "").strip():
        missing.append("tz")

    enabled = city.get("city_enabled", "").strip().lower()
    if enabled not in ("true", "1", "yes"):
        return False, "city_enabled is not true"

    if missing:
        return False, f"missing metadata: {', '.join(missing)}"

    return True, ""


def check_station_available(city: dict) -> bool:
    """檢查城市是否有 station（A 來源需要）。"""
    return bool(city.get("station_code", "").strip())


# ============================================================
# D 來源：最早月份探測
# ============================================================

def check_forecast_day_completeness(hourly: dict, target_date: str) -> tuple[bool, str]:
    """
    檢查某一天 D ���源 8 層是否都完整。

    規則：
    1. D_REQUIRED_VARS 裡 8 個變數都必須存在
    2. 該日期每個變數的 non-null hourly 筆數 = 該日期實際 timestamp 數量
       （不硬寫�� 24，因為 DST 切換時某天可能不是 24 小時）

    回傳 (is_complete, note)。
    """
    times = hourly.get("time", [])
    if not times:
        return False, "no_hourly_time"

    # 用 hourly.time 算出該日期有幾個 timestamp
    date_indices = [i for i, t in enumerate(times) if t[:10] == target_date]
    expected_count = len(date_indices)
    if expected_count == 0:
        return False, f"no_timestamps_for_{target_date}"

    missing_vars = []
    incomplete_vars = []

    for var in D_REQUIRED_VARS:
        vals = hourly.get(var)
        if vals is None or len(vals) == 0:
            short = var.replace("temperature_2m_", "") if "previous" in var else "day0"
            missing_vars.append(short)
            continue

        non_null = sum(1 for i in date_indices if i < len(vals) and vals[i] is not None)
        if non_null != expected_count:
            short = var.replace("temperature_2m_", "") if "previous" in var else "day0"
            incomplete_vars.append(f"{short}({non_null}/{expected_count})")

    if missing_vars:
        return False, f"missing_vars={','.join(missing_vars)}"
    if incomplete_vars:
        return False, f"incomplete_hourly: {','.join(incomplete_vars)}"

    return True, f"complete: 8_layers x {expected_count}h"


def probe_openmeteo_date(lat: float, lon: float, model: str, target_date: str) -> tuple[bool, str]:
    """
    對 Open-Meteo Historical Forecast API 探測某天 8 層是否完整可用。
    請求全部 8 個 required vars，用 check_forecast_day_completeness 判定。
    回傳 (is_complete, note)。
    """
    import requests

    try:
        resp = requests.get(OM_HISTORICAL_FORECAST_URL, params={
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(D_REQUIRED_VARS),
            "start_date": target_date,
            "end_date": target_date,
            "models": model,
        }, timeout=REQUEST_TIMEOUT)

        if resp.status_code != 200:
            return False, f"probe_http_{resp.status_code}"

        data = resp.json()
        hourly = data.get("hourly", {})
        return check_forecast_day_completeness(hourly, target_date)

    except Exception as e:
        return False, f"probe_request_failed: {str(e)[:80]}"


def find_earliest_month_for_model(
    lat: float,
    lon: float,
    model: str,
    current_year: int,
    verbose: bool = False,
) -> tuple[Optional[str], str]:
    """
    D 來源的最早月份探測規則（嚴格版：要求 8 層都完整）：

    1. 從 current_year 的 01-01 開始
    2. 8 層完整就往���一年試
    3. 某年 01-01 不完整，就在那一年內逐月 01 號往後試
    4. 找到第一個完整月份 → 回傳 ("YYYY-MM", note)
    5. 都沒有 → 回�� (None, last_note)
    """
    # Phase 1: 年級別往回探測
    year = current_year
    last_good_year = None
    last_note = ""

    while year >= 2020:  # Open-Meteo Historical Forecast 最早約 2022
        test_date = f"{year}-01-01"
        if verbose:
            log.info(f"    probe {model} @ {test_date} ...")

        is_complete, note = probe_openmeteo_date(lat, lon, model, test_date)
        last_note = note
        time.sleep(PROBE_DELAY)

        if is_complete:
            last_good_year = year
            year -= 1
        else:
            break

    if last_good_year is None:
        # �� current_year 的 01-01 都不完整
        # 嘗試在 current_year 逐月探
        for month in range(2, 13):
            test_date = f"{current_year}-{month:02d}-01"
            if verbose:
                log.info(f"    probe {model} @ {test_date} ...")

            is_complete, note = probe_openmeteo_date(lat, lon, model, test_date)
            last_note = note
            time.sleep(PROBE_DELAY)

            if is_complete:
                return f"{current_year}-{month:02d}", note

        return None, last_note

    # Phase 2: 在 last_good_year 的前一年（不完整的那年），逐月往後找
    failed_year = last_good_year - 1

    # 先檢查 failed_year 是否有效（>= 2020）
    if failed_year < 2020:
        # 我們探到底了，就用 last_good_year-01
        return f"{last_good_year}-01", last_note

    # failed_year 的 01-01 已知不完整，從 02 月開始試
    for month in range(2, 13):
        test_date = f"{failed_year}-{month:02d}-01"
        if verbose:
            log.info(f"    probe {model} @ {test_date} ...")

        is_complete, note = probe_openmeteo_date(lat, lon, model, test_date)
        last_note = note
        time.sleep(PROBE_DELAY)

        if is_complete:
            return f"{failed_year}-{month:02d}", note

    # failed_year 全年都不完整，那 last_good_year-01 就是最早
    return f"{last_good_year}-01", last_note


# ============================================================
# A 來源：IEM 靜態規則
# ============================================================

def probe_iem_availability(station: str) -> tuple[bool, str, str]:
    """
    A 來源用靜態規則：若 station 有值就 available。
    回傳 (available, earliest_month, note)。
    """
    if not station:
        return False, "", "no station code"

    # IEM 資料很長，用靜態常數
    return True, A_STATIC_EARLIEST, "static_rule: IEM long-history"


# ============================================================
# C 來源：ERA5 / Archive 靜態規則
# ============================================================

def probe_era5_availability(lat: str, lon: str) -> tuple[bool, str, str]:
    """
    C 來源用靜態規則：若 lat/lon 有值就 available。
    Open-Meteo Archive API 覆蓋全球，只需座標。
    回傳 (available, earliest_month, note)。
    """
    if not lat or not lon:
        return False, "", "missing lat/lon"

    return True, C_STATIC_EARLIEST, "static_rule: ERA5/Archive long-history"


# ============================================================
# 矩陣 cache / resume helpers
# ============================================================

SOURCE_ORDER = {SOURCE_A: 0, SOURCE_C: 1, SOURCE_D: 2}
DECISION_FIELDS = ["fetch_enabled", "fetch_start_month", "fetch_end_month"]


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc_timestamp(value: str) -> Optional[datetime]:
    value = value.strip()
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def safe_int(value: str, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def normalize_existing_row(
    row: Optional[dict] = None,
    *,
    city: str = "",
    source_type: str = "",
    model: str = "",
) -> dict:
    normalized = {field: "" for field in MATRIX_FIELDS}
    normalized["probe_status"] = PROBE_STATUS_PENDING
    normalized["probe_attempts"] = "0"

    if row:
        for field in MATRIX_FIELDS:
            if field in row and row[field] is not None:
                normalized[field] = str(row[field])

    if city:
        normalized["city"] = city
    if source_type:
        normalized["source_type"] = source_type
    if model:
        normalized["model"] = model

    if not normalized["probe_status"].strip():
        normalized["probe_status"] = PROBE_STATUS_PENDING
    if not normalized["probe_attempts"].strip():
        normalized["probe_attempts"] = "0"

    return normalized


def row_key(row: dict) -> tuple[str, str, str]:
    return (
        row.get("city", "").strip(),
        row.get("source_type", "").strip(),
        row.get("model", "").strip(),
    )


def load_existing_matrix(csv_path: Path) -> dict[tuple, dict]:
    """讀取舊矩陣，回傳 {(city, source_type, model): normalized_row}。"""
    if not csv_path.exists():
        return {}

    result = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for raw_row in reader:
            normalized = normalize_existing_row(raw_row)
            key = row_key(normalized)
            result[key] = normalized

    log.info(f"從舊矩陣讀到 {len(result)} 筆")
    return result


def sort_matrix_rows(rows: list[dict]) -> list[dict]:
    return sorted(
        rows,
        key=lambda row: (
            row.get("city", "").strip(),
            SOURCE_ORDER.get(row.get("source_type", "").strip(), 99),
            row.get("model", "").strip(),
        ),
    )


def write_matrix_csv(rows: list[dict], csv_path: Path) -> None:
    """寫入矩陣 CSV。"""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MATRIX_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in sort_matrix_rows(rows):
            writer.writerow(row)

    log.info(f"矩陣 CSV 已寫入: {csv_path}（{len(rows)} 列）")


def persist_matrix_store(matrix_store: dict[tuple, dict], csv_path: Path) -> None:
    write_matrix_csv(list(matrix_store.values()), csv_path)


def build_target_row_keys(
    cities: list[dict],
    d_models_to_probe: list[str],
) -> list[tuple[str, str, str]]:
    keys: list[tuple[str, str, str]] = []
    for city_data in cities:
        city = city_data.get("city", "").strip()
        keys.append((city, SOURCE_A, ""))
        keys.append((city, SOURCE_C, ""))
        for model in d_models_to_probe:
            keys.append((city, SOURCE_D, model))
    return keys


def seed_target_rows(
    matrix_store: dict[tuple, dict],
    target_keys: list[tuple[str, str, str]],
) -> int:
    created = 0
    for city, source_type, model in target_keys:
        key = (city, source_type, model)
        if key not in matrix_store:
            matrix_store[key] = normalize_existing_row(
                city=city,
                source_type=source_type,
                model=model,
            )
            created += 1
    return created


def has_explicit_available_value(row: dict) -> bool:
    return row.get("available", "").strip().lower() in EXPLICIT_AVAILABLE_VALUES


def is_probe_complete(row: dict, current_year: int) -> bool:
    return (
        row.get("probe_status", "").strip().lower() == PROBE_STATUS_DONE
        and has_explicit_available_value(row)
        and row.get("probe_basis_year", "").strip() == str(current_year)
        and row.get("probe_version", "").strip() == PROBE_VERSION
    )


def is_running_row_stale(row: dict, now_dt: datetime) -> bool:
    latest_probe_time = parse_utc_timestamp(row.get("latest_probe_time", ""))
    if latest_probe_time is None:
        return True
    return (now_dt - latest_probe_time).total_seconds() > PROBE_RUNNING_STALE_SECONDS


def classify_probe_action(row: dict, current_year: int, now_dt: datetime) -> tuple[str, str]:
    if is_probe_complete(row, current_year):
        return "reuse_done", "probe already done for current version/year"

    status = row.get("probe_status", "").strip().lower()

    if status == PROBE_STATUS_RUNNING:
        if is_running_row_stale(row, now_dt):
            return "rerun_stale_running", "stale running row"
        return "rerun_running", "recover running row from previous incomplete invocation"
    if status == PROBE_STATUS_FAILED:
        return "rerun_failed", "previous attempt failed"
    if status == PROBE_STATUS_PENDING or not status:
        return "rerun_pending", "pending row"
    if status == PROBE_STATUS_SKIPPED:
        return "rerun_skipped", "previous row marked skipped"
    if status == PROBE_STATUS_DONE:
        return "rerun_incomplete_done", "done row missing current year/version/available"
    return "rerun_unknown", f"status={status or '<empty>'}"


def mark_row_running(row: dict, current_year: int) -> None:
    row["available"] = ""
    row["earliest_month"] = ""
    row["probe_rule"] = ""
    row["note"] = ""
    row["latest_probe_time"] = now_utc_str()
    row["probe_status"] = PROBE_STATUS_RUNNING
    row["probe_completed_at"] = ""
    row["probe_version"] = PROBE_VERSION
    row["probe_basis_year"] = str(current_year)
    row["probe_attempts"] = str(safe_int(row.get("probe_attempts", "0")) + 1)


def finalize_probe_row(
    row: dict,
    *,
    current_year: int,
    status: str,
    available: Optional[bool],
    earliest_month: str,
    probe_rule: str,
    note: str,
) -> None:
    finished_at = now_utc_str()
    row["available"] = "" if available is None else str(available)
    row["earliest_month"] = earliest_month or ""
    row["latest_probe_time"] = finished_at
    row["probe_rule"] = probe_rule
    row["note"] = note
    row["probe_status"] = status
    row["probe_completed_at"] = finished_at
    row["probe_version"] = PROBE_VERSION
    row["probe_basis_year"] = str(current_year)


def probe_matrix_row(
    city_data: dict,
    source_type: str,
    model: str,
    current_year: int,
    verbose: bool = False,
) -> dict:
    city_name = city_data.get("city", "").strip()
    lat = city_data.get("lat", "").strip()
    lon = city_data.get("lon", "").strip()
    station = city_data.get("station_code", "").strip()

    meta_ok, meta_note = check_city_metadata(city_data)

    if source_type == SOURCE_A:
        if meta_ok and station:
            available, earliest, note = probe_iem_availability(station)
        elif not station:
            available, earliest, note = False, "", "no station code"
        else:
            available, earliest, note = False, "", meta_note
        return {
            "status": PROBE_STATUS_DONE,
            "available": available,
            "earliest_month": earliest,
            "probe_rule": "static_rule",
            "note": note,
        }

    if source_type == SOURCE_C:
        if meta_ok:
            available, earliest, note = probe_era5_availability(lat, lon)
        else:
            available, earliest, note = False, "", meta_note
        return {
            "status": PROBE_STATUS_DONE,
            "available": available,
            "earliest_month": earliest,
            "probe_rule": "static_rule",
            "note": note,
        }

    if source_type == SOURCE_D:
        if not meta_ok:
            return {
                "status": PROBE_STATUS_DONE,
                "available": False,
                "earliest_month": "",
                "probe_rule": "skipped",
                "note": meta_note,
            }

        if verbose:
            log.info(f"  探測 {city_name} × {model} ...")

        try:
            earliest, probe_note = find_earliest_month_for_model(
                lat=float(lat),
                lon=float(lon),
                model=model,
                current_year=current_year,
                verbose=verbose,
            )
            if earliest:
                return {
                    "status": PROBE_STATUS_DONE,
                    "available": True,
                    "earliest_month": earliest,
                    "probe_rule": "8_layer_strict",
                    "note": f"8_layers_complete from {earliest} ({probe_note})",
                }
            return {
                "status": PROBE_STATUS_DONE,
                "available": False,
                "earliest_month": "",
                "probe_rule": "8_layer_strict",
                "note": probe_note or "probe found no complete data",
            }
        except Exception as exc:
            return {
                "status": PROBE_STATUS_FAILED,
                "available": None,
                "earliest_month": "",
                "probe_rule": "8_layer_strict",
                "note": f"probe error: {str(exc)[:100]}",
            }

    return {
        "status": PROBE_STATUS_FAILED,
        "available": None,
        "earliest_month": "",
        "probe_rule": "unknown",
        "note": f"unknown source_type: {source_type}",
    }


# ============================================================
# 主流程
# ============================================================

def run(
    city_csv: Optional[Path] = None,
    output_path: Optional[Path] = None,
    city_filter: Optional[list[str]] = None,
    d_model_filter: Optional[list[str]] = None,
    current_year: Optional[int] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> list[dict]:
    """主流程：讀城市 → probe cache check / resume → 寫檔"""

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    city_csv_path = city_csv or DEFAULT_CITY_CSV
    matrix_csv_path = output_path or DEFAULT_MATRIX_CSV

    if current_year is None:
        current_year = date.today().year

    try:
        resolved_d_models, skipped_disabled = resolve_active_d_models(d_model_filter)
    except ValueError as exc:
        log.error(str(exc))
        return []

    log.info("=" * 50)
    log.info("03_get_data_matrix: 資料可用性矩陣產生器")
    log.info(f"  城市來源: {city_csv_path}")
    log.info(f"  輸出: {matrix_csv_path}")
    log.info(f"  探測基準年: {current_year}")
    log.info(f"  probe_version: {PROBE_VERSION}")
    log.info(f"  running stale threshold: {PROBE_RUNNING_STALE_SECONDS}s")
    log.info(f"  D 模型篩選（輸入）: {d_model_filter if d_model_filter else '<default active>'}")
    log.info(f"  D 模型篩選（生效）: {resolved_d_models if resolved_d_models else '<none>'}")
    if d_model_filter:
        for item in skipped_disabled:
            log.warning(
                "  D 模型已停用並略過: "
                f"{item['token']} -> {item['model']} ({item['reason']})"
            )
    elif skipped_disabled:
        log.info(
            "  預設排除停用模型: "
            + ", ".join(f"{item['alias']}={item['model']}" for item in skipped_disabled)
        )
    log.info("=" * 50)

    cities = load_city_csv(city_csv_path)
    if not cities:
        log.error("沒有城市可處理")
        return []

    filtered = filter_enabled_cities(cities, city_filter=city_filter)
    log.info(f"篩選後: {len(filtered)} 個城市")
    if not filtered:
        log.warning("篩選後沒有城市可處理")
        return []

    target_keys = build_target_row_keys(filtered, resolved_d_models)
    city_lookup = {city["city"].strip(): city for city in filtered}

    matrix_store = load_existing_matrix(matrix_csv_path)
    seeded = seed_target_rows(matrix_store, target_keys)
    log.info(f"本輪 target rows: {len(target_keys)}")
    if seeded:
        log.info(f"新增 pending rows: {seeded}")

    if not dry_run:
        persist_matrix_store(matrix_store, matrix_csv_path)

    stats = {
        "reuse_done": 0,
        "rerun_pending": 0,
        "rerun_failed": 0,
        "rerun_running": 0,
        "rerun_stale_running": 0,
        "rerun_skipped": 0,
        "rerun_incomplete_done": 0,
        "rerun_unknown": 0,
        "probed_done": 0,
        "probed_failed": 0,
    }

    for city, source_type, model in target_keys:
        key = (city, source_type, model)
        row = matrix_store[key]
        action, reason = classify_probe_action(
            row,
            current_year=current_year,
            now_dt=datetime.now(timezone.utc),
        )

        if action == "reuse_done":
            stats[action] += 1
            if verbose:
                log.info(f"reuse done: {city} {source_type} {model or '<none>'}")
            continue

        stats[action] += 1
        log.info(
            f"probe row: {city} {source_type}"
            + (f" {model}" if model else "")
            + f" [{action}] {reason}"
        )

        if dry_run:
            continue

        mark_row_running(row, current_year=current_year)
        persist_matrix_store(matrix_store, matrix_csv_path)

        result = probe_matrix_row(
            city_data=city_lookup[city],
            source_type=source_type,
            model=model,
            current_year=current_year,
            verbose=verbose,
        )
        finalize_probe_row(
            row,
            current_year=current_year,
            status=result["status"],
            available=result["available"],
            earliest_month=result["earliest_month"],
            probe_rule=result["probe_rule"],
            note=result["note"],
        )
        persist_matrix_store(matrix_store, matrix_csv_path)

        if result["status"] == PROBE_STATUS_DONE:
            stats["probed_done"] += 1
        else:
            stats["probed_failed"] += 1

    log.info("Probe summary:")
    log.info(f"  reused done rows: {stats['reuse_done']}")
    log.info(f"  rerun pending rows: {stats['rerun_pending']}")
    log.info(f"  rerun failed rows: {stats['rerun_failed']}")
    log.info(f"  recovered running rows: {stats['rerun_running']}")
    log.info(f"  recovered stale running rows: {stats['rerun_stale_running']}")
    log.info(f"  rerun skipped rows: {stats['rerun_skipped']}")
    log.info(f"  rerun incomplete done rows: {stats['rerun_incomplete_done']}")
    log.info(f"  rerun unknown rows: {stats['rerun_unknown']}")
    log.info(f"  newly completed rows: {stats['probed_done']}")
    log.info(f"  failed rows this run: {stats['probed_failed']}")
    log.info("完成。")

    return sort_matrix_rows(list(matrix_store.values()))


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="讀取 01_city.csv，探測每個城市×來源×模型的資料可用性，產生矩陣 CSV",
    )
    p.add_argument("--cities", type=str, default=None,
                   help="只處理指定城市（逗號分隔，例如 London,Paris）")
    p.add_argument("--d-models", type=str, default=None,
                   help="只探測指定 D 模型（alias 或完整名稱；D13/D14/D18 會被標記為 disabled 並略過）")
    p.add_argument("--output", type=str, default=None,
                   help="輸出 CSV 路徑（預設: 同目錄 03_get_data_matrix.csv）")
    p.add_argument("--current-year", type=int, default=None,
                   help="探測基準年（預設: 今年）")
    p.add_argument("--dry-run", action="store_true",
                   help="只印出摘要，不寫檔")
    p.add_argument("--verbose", action="store_true",
                   help="詳細 log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()

    city_filter = None
    if args.cities:
        city_filter = [c.strip() for c in args.cities.split(",") if c.strip()]

    d_model_filter = None
    if args.d_models:
        d_model_filter = [m.strip() for m in args.d_models.split(",") if m.strip()]

    run(
        output_path=Path(args.output) if args.output else None,
        city_filter=city_filter,
        d_model_filter=d_model_filter,
        current_year=args.current_year,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
