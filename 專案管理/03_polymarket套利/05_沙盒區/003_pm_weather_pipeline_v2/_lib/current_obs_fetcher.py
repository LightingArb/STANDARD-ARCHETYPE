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

    def fetch_current(self, city: str, station_code: str) -> Optional[dict]:
        """
        純 API call，不使用內部快取。供 collector_main 的 _run_obs_fetch 使用。
        回傳 {high_c, current_temp_c, obs_time, source} 或 None（失敗時）。
        """
        try:
            result = self._fetch_v3_current(station_code)
        except Exception as e:
            log.warning(f"fetch_current {city} ({station_code}): {e}")
            return None
        if result is None:
            return None
        return {
            "high_c": result["high_c"],
            "current_temp_c": result.get("current_temp_c"),
            "obs_time": result["obs_time"],
            "source": "v3_current",
        }

    def _fetch_v3_current(self, station_code: str) -> Optional[dict]:
        """
        呼叫 WU v3 current observations by ICAO code。
        回傳 {high_c, obs_time} 或 None（失敗時）。
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

        current_temp = data.get("temperature")
        return {
            "high_c": float(high_c),
            "current_temp_c": float(current_temp) if current_temp is not None else None,
            "obs_time": str(data.get("validTimeUtc", "")),
        }

    def get_current_high(
        self,
        city: str,
        station_code: str,
        hours_to_settlement: Optional[float] = None,
    ) -> Optional[dict]:
        """
        取得城市今天目前最高溫（攝氏），帶快取。
        hours_to_settlement: 距結算剩餘小時數，< settling_threshold 時使用較短的 TTL。
        回傳 {high_c, fetched_at, obs_time, source} 或 None（失敗時）。
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
            "obs_time": result["obs_time"],
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
        """
        批次取得所有城市的即時最高溫。

        cities_info: list of {city, station_code}
        hours_to_settlement_map: {city: float}（距結算小時數，用於動態 TTL）
        回傳: {city: {high_c, fetched_at, obs_time, source}}
        失敗的城市靜默跳過，不在結果中。
        """
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
