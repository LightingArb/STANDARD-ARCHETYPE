"""
13_city_status_manager.py — 城市狀態機

管理 data/city_status.json，追蹤每個城市從被掃描到可交易的生命週期。

狀態值：
  discovered    → 掃描到了，有 metadata，等待回補
  backfilling   → 正在回補歷史資料
  ready         → 歷史資料足夠，已解鎖（可接收 live signal）
  disabled      → 手動停用
  no_metadata   → 缺 station metadata，需人工補 seed
  failed        → 回補失敗

CLI：
  python 13_city_status_manager.py --list
  python 13_city_status_manager.py --ready
  python 13_city_status_manager.py --set London disabled "暫停交易"
"""

import argparse
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

DEFAULT_STATUS_PATH = PROJ_DIR / "data" / "city_status.json"

# ============================================================
# 合法狀態轉移表
# ============================================================

VALID_TRANSITIONS: dict[str, set[str]] = {
    "discovered":  {"backfilling", "disabled"},
    "backfilling": {"ready", "failed", "discovered"},  # discovered: recovery from stuck
    "failed":      {"backfilling", "discovered"},       # retry or full reset
    "ready":       {"disabled"},
    "no_metadata": {"discovered"},          # metadata 補齊後升級
    "disabled":    set(),                  # terminal unless manually overridden via --force
}

ALL_STATUSES = {"discovered", "backfilling", "ready", "disabled", "no_metadata", "failed"}

SCHEMA_VERSION = "city_status_v1"
MIN_ERROR_ROWS_BOOTSTRAP = 730   # bootstrap() ready 條件（約 2 年，覆蓋完整夏冬循環）
READY_MAX_FORECAST_AGE_DAYS = 7  # latest_forecast_date 距今最多幾天


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ============================================================
# 原子寫入
# ============================================================

def _save_status(path: Path, data: dict) -> None:
    """先寫 temp file，再 os.replace（原子寫入，防止讀到半寫狀態）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    dir_path = str(path.parent)
    with tempfile.NamedTemporaryFile(
        "w", dir=dir_path, suffix=".tmp", delete=False, encoding="utf-8"
    ) as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        tmp_path = f.name
    os.replace(tmp_path, str(path))


# ============================================================
# CityStatusManager
# ============================================================

class CityStatusManager:
    """
    城市狀態機。所有狀態變更都透過此 class 進行，確保：
    1. 狀態轉移合法性檢查
    2. 原子 JSON 寫入
    3. updated_at_utc 自動更新
    """

    def __init__(self, path: Path = DEFAULT_STATUS_PATH):
        self.path = path
        self._data: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self._data = {}
            return
        try:
            self._data = json.loads(self.path.read_text("utf-8"))
        except Exception as e:
            log.warning(f"Could not load {self.path}: {e} — starting fresh")
            self._data = {}

    def _save(self) -> None:
        _save_status(self.path, self._data)

    def _transition(self, city: str, new_status: str, force: bool = False) -> None:
        """驗證並執行狀態轉移。非法轉移 raise ValueError。"""
        current = self._data.get(city, {}).get("status", None)
        if current == new_status:
            return  # no-op
        if current is not None and not force:
            allowed = VALID_TRANSITIONS.get(current, set())
            if new_status not in allowed:
                raise ValueError(
                    f"Invalid transition for {city}: {current!r} → {new_status!r}. "
                    f"Allowed: {sorted(allowed) or '(none)'}"
                )
        if city not in self._data:
            self._data[city] = {}
        self._data[city]["status"] = new_status
        self._data[city]["updated_at_utc"] = _now_utc()

    # ── Read API ──────────────────────────────────────────────

    def get_status(self, city: str) -> Optional[str]:
        return self._data.get(city, {}).get("status")

    def get_ready_cities(self) -> list[str]:
        return sorted(c for c, d in self._data.items() if d.get("status") == "ready")

    def get_cities_by_status(self, status: str) -> list[str]:
        return sorted(c for c, d in self._data.items() if d.get("status") == status)

    def get_all(self) -> dict:
        return dict(self._data)

    def city_exists(self, city: str) -> bool:
        return city in self._data

    # ── Write API ─────────────────────────────────────────────

    def set_discovered(self, city: str, metadata: dict, force: bool = False) -> None:
        """
        新建或升級（no_metadata → discovered）城市。
        metadata 應包含 timezone, station_code, country, supported_metrics 等。
        """
        current = self._data.get(city, {}).get("status")
        if current == "disabled" and not force:
            log.warning(f"{city}: skipping set_discovered, city is disabled")
            return
        if current == "discovered":
            # already discovered — just refresh metadata
            self._data[city].update({k: v for k, v in metadata.items()})
            self._data[city]["updated_at_utc"] = _now_utc()
            self._save()
            return
        self._transition(city, "discovered", force=force)
        now = _now_utc()
        existing = self._data.get(city, {})
        self._data[city] = {
            "schema_version": SCHEMA_VERSION,
            "status": "discovered",
            "city": city,
            "timezone": metadata.get("timezone", ""),
            "station_code": metadata.get("station_code", ""),
            "country": metadata.get("country", ""),
            "supported_metrics": metadata.get("supported_metrics", ["daily_high"]),
            "discovered_at_utc": existing.get("discovered_at_utc", now),
            "last_scan_at_utc": now,
            "last_backfill_start_utc": None,
            "last_backfill_end_utc": None,
            "last_ready_utc": None,
            "earliest_forecast_date": None,
            "latest_forecast_date": None,
            "error_row_count": 0,
            "market_count_active": metadata.get("market_count_active", 0),
            "failure_count": existing.get("failure_count", 0),
            "last_error": None,
            "probability_build_time_utc": None,
            "model_build_time_utc": None,
            "note": metadata.get("note", ""),
            "updated_at_utc": now,
        }
        self._save()

    def set_no_metadata(self, city: str) -> None:
        """城市掃描到但缺 seed metadata。"""
        if city not in self._data:
            now = _now_utc()
            self._data[city] = {
                "schema_version": SCHEMA_VERSION,
                "status": "no_metadata",
                "city": city,
                "timezone": "",
                "station_code": "",
                "country": "",
                "supported_metrics": [],
                "discovered_at_utc": now,
                "last_scan_at_utc": now,
                "last_backfill_start_utc": None,
                "last_backfill_end_utc": None,
                "last_ready_utc": None,
                "earliest_forecast_date": None,
                "latest_forecast_date": None,
                "error_row_count": 0,
                "market_count_active": 0,
                "failure_count": 0,
                "last_error": None,
                "probability_build_time_utc": None,
                "model_build_time_utc": None,
                "note": "missing station metadata — add to seed_cities.json",
                "updated_at_utc": now,
            }
            self._save()
        elif self._data[city].get("status") not in ("discovered", "backfilling", "ready"):
            # Already no_metadata — just update scan time
            self._data[city]["last_scan_at_utc"] = _now_utc()
            self._data[city]["updated_at_utc"] = _now_utc()
            self._save()

    def set_backfilling(self, city: str, force: bool = False) -> None:
        self._transition(city, "backfilling", force=force)
        self._data[city]["last_backfill_start_utc"] = _now_utc()
        self._save()

    def set_ready(
        self,
        city: str,
        error_row_count: int,
        earliest_forecast_date: Optional[str] = None,
        latest_forecast_date: Optional[str] = None,
        force: bool = False,
    ) -> None:
        self._transition(city, "ready", force=force)
        now = _now_utc()
        self._data[city]["last_backfill_end_utc"] = now
        self._data[city]["last_ready_utc"] = now
        self._data[city]["last_backfill_completed_utc"] = now
        self._data[city]["error_row_count"] = error_row_count
        if earliest_forecast_date is not None:
            self._data[city]["earliest_forecast_date"] = earliest_forecast_date
        if latest_forecast_date is not None:
            self._data[city]["latest_forecast_date"] = latest_forecast_date
        # 清 stale metadata（回補成功後這些欄位已無意義）
        for key in ["last_error", "note", "fail_reason", "failure_count",
                    "recovery_reason", "recovery_at_utc"]:
            self._data[city].pop(key, None)
        self._save()

    def set_failed(self, city: str, reason: str = "", force: bool = False) -> None:
        self._transition(city, "failed", force=force)
        existing_count = self._data[city].get("failure_count", 0)
        self._data[city]["failure_count"] = existing_count + 1
        self._data[city]["note"] = reason or self._data[city].get("note", "")
        self._data[city]["last_error"] = reason or None
        self._save()

    def reset_failed_cities(self) -> list[str]:
        """
        把所有 status=failed 的城市重置為 discovered（failure_count 歸零、last_error 清空）。
        由 collector_main 在每次城市掃描成功後呼叫，讓 backfill 自動重試。
        回傳被重置的城市清單。
        """
        reset = []
        for city, entry in self._data.items():
            if entry.get("status") == "failed":
                self._transition(city, "discovered")
                self._data[city]["failure_count"] = 0
                self._data[city]["last_error"] = None
                reset.append(city)
        if reset:
            self._save()
        return reset

    def reset_stuck_backfilling(self, max_age_hours: int = 4) -> list[str]:
        """
        把卡住的 backfilling 城市重置為 discovered。
        判斷：backfilling 且 last_backfill_start_utc 超過 max_age_hours，或欄位不存在。
        設計假設：單實例 collector，不允許並行手動 backfill。
        回傳被重置的城市清單。
        """
        reset = []
        now = datetime.now(timezone.utc)
        for city, info in self._data.items():
            if city.startswith("_"):
                continue
            if info.get("status") != "backfilling":
                continue
            start_str = info.get("last_backfill_start_utc", "")
            is_stuck = True
            if start_str:
                try:
                    start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    age_hours = (now - start_dt).total_seconds() / 3600
                    is_stuck = age_hours > max_age_hours
                except Exception:
                    is_stuck = True
            if is_stuck:
                try:
                    self._transition(city, "discovered")
                    self._data[city]["recovery_reason"] = "stuck_backfilling_timeout"
                    self._data[city]["recovery_at_utc"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                    reset.append(city)
                    log.info(f"  {city}: backfilling → discovered (stuck >{max_age_hours}h)")
                except ValueError as e:
                    log.warning(f"  {city}: reset_stuck failed: {e}")
        if reset:
            self._save()
        return reset

    def set_disabled_batch(self, cities: list[str], reason: str = "") -> list[str]:
        """批量設定城市為 disabled（允許從任何狀態強制進入）。"""
        done = []
        for city in cities:
            if city not in self._data:
                continue
            current = self._data[city].get("status")
            if current == "disabled":
                continue
            self._data[city]["status"] = "disabled"
            self._data[city]["updated_at_utc"] = _now_utc()
            self._data[city]["disabled_at_utc"] = _now_utc()
            if reason:
                self._data[city]["disabled_reason"] = reason
            done.append(city)
            log.info(f"  {city}: {current} → disabled ({reason})")
        if done:
            self._save()
        return done

    def set_disabled(self, city: str, reason: str = "", force: bool = False) -> None:
        self._transition(city, "disabled", force=force)
        if reason:
            self._data[city]["note"] = reason
        self._save()

    def update_scan_info(
        self,
        city: str,
        market_count_active: int,
        last_scan_at_utc: Optional[str] = None,
    ) -> None:
        """更新掃描 metadata，不改變 status。"""
        if city not in self._data:
            return
        self._data[city]["market_count_active"] = market_count_active
        self._data[city]["last_scan_at_utc"] = last_scan_at_utc or _now_utc()
        self._data[city]["updated_at_utc"] = _now_utc()
        self._save()

    def update_build_time(self, city: str, build_type: str) -> None:
        """
        更新城市的 build metadata 時間戳。
        build_type: "probability" → probability_build_time_utc
                    "model"       → model_build_time_utc
        """
        if city not in self._data:
            return
        field_map = {
            "probability": "probability_build_time_utc",
            "model": "model_build_time_utc",
        }
        field = field_map.get(build_type)
        if not field:
            log.warning(f"update_build_time: unknown build_type={build_type!r}")
            return
        self._data[city][field] = _now_utc()
        self._data[city]["updated_at_utc"] = _now_utc()
        self._save()

    def bootstrap(self, seed_cities_path: str = "config/seed_cities.json") -> list[str]:
        """
        首次使用 city_status.json 時，掃描已有資料的城市，自動標 ready。

        條件：
          1. error_table 存在且 >= MIN_ERROR_ROWS_BOOTSTRAP 筆
          2. empirical_model.json 存在且可讀
          3. city_status.json 裡還不存在的城市

        回傳：被自動 init 的城市清單。
        """
        import csv as _csv
        from datetime import date as _date

        error_root = PROJ_DIR / "data" / "processed" / "error_table"
        if not error_root.exists():
            return []

        # Load seed for metadata fallback
        seed_path = PROJ_DIR / seed_cities_path
        seed: dict = {}
        if seed_path.exists():
            try:
                seed = json.loads(seed_path.read_text("utf-8"))
            except Exception:
                pass

        auto_inited: list[str] = []

        for city_dir in sorted(error_root.iterdir()):
            if not city_dir.is_dir():
                continue
            city = city_dir.name
            if self.city_exists(city):
                continue  # already tracked — never overwrite

            # Count error_table rows
            error_path = city_dir / "market_day_error_table.csv"
            if not error_path.exists():
                continue
            try:
                with open(error_path, "r", encoding="utf-8", newline="") as f:
                    error_rows = sum(1 for _ in _csv.DictReader(f))
            except Exception:
                continue
            if error_rows < MIN_ERROR_ROWS_BOOTSTRAP:
                continue

            # Check latest error date freshness
            try:
                with open(error_path, "r", encoding="utf-8", newline="") as f:
                    dates_in_table = [
                        row["market_date_local"]
                        for row in _csv.DictReader(f)
                        if row.get("market_date_local")
                    ]
                if dates_in_table:
                    latest_date = max(dates_in_table)
                    age_days = (_date.today() - _date.fromisoformat(latest_date)).days
                    if age_days > READY_MAX_FORECAST_AGE_DAYS:
                        log.info(
                            f"bootstrap: {city} skipped — latest_date={latest_date}"
                            f" is {age_days}d old (max={READY_MAX_FORECAST_AGE_DAYS})"
                        )
                        continue
            except Exception:
                pass

            # Check empirical_model.json
            model_path = PROJ_DIR / "data" / "models" / "empirical" / city / "empirical_model.json"
            if not model_path.exists():
                continue
            try:
                json.loads(model_path.read_text("utf-8"))
            except Exception:
                continue

            # All conditions met → auto-init as ready
            city_seed = seed.get(city, {})
            metadata = {
                "timezone": city_seed.get("timezone", "UTC"),
                "station_code": city_seed.get("station_code", ""),
                "country": city_seed.get("country", ""),
                "supported_metrics": ["daily_high"],
            }
            self.auto_init_ready(
                city=city,
                metadata=metadata,
                error_row_count=error_rows,
            )
            auto_inited.append(city)
            log.info(f"bootstrap: {city} → ready (error_rows={error_rows})")

        return auto_inited

    def auto_init_ready(
        self,
        city: str,
        metadata: dict,
        error_row_count: int,
        earliest_forecast_date: Optional[str] = None,
        latest_forecast_date: Optional[str] = None,
    ) -> None:
        """
        自動將已有歷史資料的城市直接設為 ready（繞過 discovered→backfilling 流程）。
        用於 London/Paris 初始化。
        """
        if city not in self._data:
            self.set_discovered(city, metadata, force=True)
            self.set_backfilling(city, force=True)
            self.set_ready(
                city,
                error_row_count=error_row_count,
                earliest_forecast_date=earliest_forecast_date,
                latest_forecast_date=latest_forecast_date,
                force=True,
            )
            log.info(f"Auto-initialized {city} as ready (error_rows={error_row_count})")
        else:
            log.info(f"City {city} already in city_status.json (status={self.get_status(city)}), skipping auto-init")


# ============================================================
# CLI
# ============================================================

def _format_table(all_data: dict, filter_status: Optional[str] = None) -> None:
    header = f"{'City':<25} {'Status':<15} {'Rows':>6} {'Markets':>8} {'Updated':<22}"
    print(header)
    print("-" * len(header))
    for city, d in sorted(all_data.items()):
        status = d.get("status", "?")
        if filter_status and status != filter_status:
            continue
        rows = d.get("error_row_count", 0) or 0
        mkts = d.get("market_count_active", 0) or 0
        updated = (d.get("updated_at_utc") or "")[:19]
        print(f"{city:<25} {status:<15} {rows:>6} {mkts:>8} {updated:<22}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="城市狀態管理 CLI",
    )
    parser.add_argument("--list", action="store_true", help="列出所有城市及狀態")
    parser.add_argument("--ready", action="store_true", help="列出所有 ready 城市")
    parser.add_argument(
        "--set", nargs="+", metavar=("CITY", "STATUS"),
        help="手動設定狀態：--set <city> <status> [reason]",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="允許非法狀態轉移（只用於手動緊急修復）",
    )
    parser.add_argument("--path", type=str, default="", help="city_status.json 路徑（可選）")
    args = parser.parse_args()

    path = Path(args.path) if args.path else DEFAULT_STATUS_PATH
    csm = CityStatusManager(path=path)

    if args.list:
        _format_table(csm.get_all())
        return

    if args.ready:
        ready = csm.get_ready_cities()
        if not ready:
            print("(no ready cities)")
        else:
            for c in ready:
                print(c)
        return

    if args.set:
        if len(args.set) < 2:
            print("Usage: --set <city> <status> [reason]", file=sys.stderr)
            sys.exit(1)
        city = args.set[0]
        new_status = args.set[1]
        reason = " ".join(args.set[2:]) if len(args.set) > 2 else ""

        if new_status not in ALL_STATUSES:
            print(f"Unknown status {new_status!r}. Valid: {sorted(ALL_STATUSES)}", file=sys.stderr)
            sys.exit(1)

        current = csm.get_status(city)
        try:
            if new_status == "disabled":
                csm.set_disabled(city, reason=reason, force=args.force)
            elif new_status == "discovered":
                csm.set_discovered(city, metadata={}, force=args.force)
            elif new_status == "backfilling":
                csm.set_backfilling(city, force=args.force)
            elif new_status == "failed":
                csm.set_failed(city, reason=reason, force=args.force)
            else:
                # ready, no_metadata handled via force
                csm._transition(city, new_status, force=args.force)
                csm._save()
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

        print(f"{city}: {current!r} → {new_status!r}" + (f" ({reason})" if reason else ""))
        return

    parser.print_help()


if __name__ == "__main__":
    main()
