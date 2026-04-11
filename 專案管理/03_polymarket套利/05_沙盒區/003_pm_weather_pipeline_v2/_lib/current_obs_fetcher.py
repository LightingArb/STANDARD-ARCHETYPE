"""
_lib/current_obs_fetcher.py — 即時觀測溫度抓取（WU v3 ICAO endpoint）

設計原則：
  1. 只用 WU v3/wx/observations/current by ICAO code
  2. 取 temperatureMaxSince7Am（今天目前最高溫）
  3. 30 分鐘快取（節省 API quota，500 req/day 限制）
  4. 失敗時靜默，不中斷 signal 主流程
"""

import logging
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

PROJ_DIR = Path(__file__).resolve().parent.parent


def load_wu_api_key() -> Optional[str]:
    """讀取 WU API key（config/wu_api_key.txt）。找不到回傳 None。"""
    key_path = PROJ_DIR / "config" / "wu_api_key.txt"
    if key_path.exists():
        key = key_path.read_text(encoding="utf-8").strip()
        return key if key else None
    log.warning(f"WU API key not found: {key_path}")
    return None


class CurrentObsFetcher:
    """
    抓取各城市今天目前最高溫（攝氏）。

    使用 WU v3/wx/observations/current by ICAO code。
    欄位：temperatureMaxSince7Am（今天最高，非跨日的 temperatureMax24Hour）。
    30 分鐘快取，避免 API quota 超用。

    Usage:
        fetcher = CurrentObsFetcher(api_key=load_wu_api_key())
        obs = fetcher.get_all_current_highs([
            {"city": "London", "station_code": "EGLC"},
            {"city": "Atlanta", "station_code": "KATL"},
        ])
        # obs = {"London": {"high_c": 22.0, ...}, ...}
    """

    def __init__(
        self,
        api_key: str,
        cache_ttl_seconds: int = 1800,
        settling_ttl_seconds: int = 600,
        settling_hours_threshold: float = 6.0,
    ):
        self.api_key = api_key
        self.cache_ttl = cache_ttl_seconds          # 正常城市 TTL（秒）
        self.settling_ttl = settling_ttl_seconds     # 結算中城市 TTL（秒）
        self.settling_threshold = settling_hours_threshold  # 距結算幾小時以下算「結算中」
        self._cache: dict[str, dict] = {}  # {city: {high_c, fetched_at, obs_time, source}}

    def _fetch_v3_current(self, station_code: str) -> Optional[dict]:
        """
        呼叫 WU v3 current observations by ICAO code。
        回傳 {high_c, current_temp_c, obs_time_utc} 或 None（失敗時）。

        WU v3 response 欄位：
          temperatureMaxSince7Am  → 今天目前最高溫（high_c）
          temperature             → 當下氣溫（current_temp_c）
          validTimeUtc            → UNIX epoch 秒（int），存成 int 而非 str
        """
        import requests
        url = (
            f"https://api.weather.com/v3/wx/observations/current"
            f"?icaoCode={station_code}"
            f"&language=en-US&format=json&units=m"
            f"&apiKey={self.api_key}"
        )
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        high_c = data.get("temperatureMaxSince7Am")
        if high_c is None:
            log.warning(
                f"temperatureMaxSince7Am missing for {station_code}, "
                f"available keys: {list(data.keys())[:10]}"
            )
            return None

        # 當下氣溫（P1-4：current_temp_c 不該等於 high_c）
        current_temp_raw = data.get("temperature")
        current_temp_c: Optional[float] = None
        if current_temp_raw is not None:
            try:
                current_temp_c = float(current_temp_raw)
            except (TypeError, ValueError):
                current_temp_c = None

        # validTimeUtc 是 epoch 秒（int）。轉成 int 而非 str
        obs_time_utc: Optional[int] = None
        valid_time_raw = data.get("validTimeUtc")
        if valid_time_raw is not None:
            try:
                obs_time_utc = int(valid_time_raw)
            except (TypeError, ValueError):
                obs_time_utc = None

        return {
            "high_c": float(high_c),
            "current_temp_c": current_temp_c,
            "obs_time_utc": obs_time_utc,  # int epoch or None
        }

    def get_current_high(
        self,
        city: str,
        station_code: str,
        hours_to_settlement: Optional[float] = None,
    ) -> Optional[dict]:
        """
        取得城市今天目前最高溫（攝氏），帶快取。

        **P1-5 fix**：fetcher 必須是長壽物件，每輪重建會讓 cache 永遠清空，TTL 形同虛設。
        collector_main 現在啟動時建一次、跨輪重用。

        hours_to_settlement: 距結算剩餘小時數，< settling_threshold 時使用較短的 TTL。
        回傳 {high_c, current_temp_c, fetched_at, obs_time_utc, source} 或 None（失敗時）。
        """
        # 動態 TTL：結算中城市用較短的 TTL
        is_settling = (
            hours_to_settlement is not None
            and hours_to_settlement < self.settling_threshold
        )
        target_ttl = self.settling_ttl if is_settling else self.cache_ttl

        now = time.time()
        cached = self._cache.get(city)
        if cached and (now - cached["fetched_at"]) < target_ttl:
            return cached

        try:
            result = self._fetch_v3_current(station_code)
        except Exception as e:
            log.warning(f"CurrentObsFetcher: {city} ({station_code}) failed: {e}")
            return None

        if result is None:
            return None

        entry = {
            "high_c": result["high_c"],
            "current_temp_c": result.get("current_temp_c"),
            "obs_time_utc": result.get("obs_time_utc"),  # int epoch or None
            "fetched_at": now,
            "source": "v3_current",
        }
        self._cache[city] = entry
        log.info(f"Current obs {city}: high={result['high_c']:.1f}°C (station={station_code})")
        return entry

    def get_all_current_highs(
        self,
        cities_info: list[dict],
        hours_to_settlement_map: Optional[dict] = None,
    ) -> dict[str, dict]:
        """序列版（保留向後相容）。新程式碼應用 get_all_parallel()。"""
        result: dict[str, dict] = {}
        for info in cities_info:
            city = info.get("city", "")
            station_code = info.get("station_code", "")
            if not city or not station_code:
                continue
            hours = (hours_to_settlement_map or {}).get(city)
            obs = self.get_current_high(city, station_code, hours_to_settlement=hours)
            if obs:
                result[city] = obs
        return result

    def get_all_parallel(
        self,
        cities_info: list[dict],
        hours_to_settlement_map: Optional[dict] = None,
        max_workers: int = 5,
        total_budget_s: float = 300.0,
    ) -> dict[str, dict]:
        """
        **P2-1**：並行批次抓取。20 城市 × 15s timeout 序列 = 5 分鐘，接近 10 分鐘觸發間隔。
        並行 + 總預算保護避免整輪卡死。

        cities_info: list of {city, station_code}
        hours_to_settlement_map: {city: float}
        max_workers: 並發數（預設 5，避免 WU API rate limit）
        total_budget_s: 整輪總時間預算（秒），超過後不再等待剩餘 future
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        result: dict[str, dict] = {}
        valid_infos = [
            i for i in cities_info
            if i.get("city") and i.get("station_code")
        ]
        if not valid_infos:
            return result

        start = time.time()

        def _worker(info: dict) -> tuple[str, Optional[dict]]:
            city = info["city"]
            station = info["station_code"]
            hours = (hours_to_settlement_map or {}).get(city)
            obs = self.get_current_high(city, station, hours_to_settlement=hours)
            return (city, obs)

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_worker, info): info for info in valid_infos}
            for fut in as_completed(futures):
                elapsed = time.time() - start
                remaining = total_budget_s - elapsed
                if remaining <= 0:
                    city = futures[fut].get("city", "?")
                    log.warning(
                        f"get_all_parallel: total budget {total_budget_s}s exhausted, "
                        f"aborting ({city} and rest)"
                    )
                    # 取消尚未啟動的 future；已跑的會跑完但 result 不收
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break
                try:
                    city, obs = fut.result(timeout=max(1.0, remaining))
                    if obs:
                        result[city] = obs
                except Exception as e:
                    info = futures[fut]
                    log.warning(f"get_all_parallel worker {info.get('city')}: {e}")

        log.info(
            f"get_all_parallel: {len(result)}/{len(valid_infos)} ok in "
            f"{time.time() - start:.1f}s"
        )
        return result
