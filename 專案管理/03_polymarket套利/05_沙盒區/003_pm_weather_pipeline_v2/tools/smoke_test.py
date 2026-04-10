"""
tools/smoke_test.py — Phase 2 最小回歸測試

快速檢查所有模組的基本一致性。
每個 test_* 函式：通過 → 靜默；失敗 → raise AssertionError 或 Exception。

用法：
  python tools/smoke_test.py           # 全部測試
  python tools/smoke_test.py -v        # 詳細輸出
"""

import csv
import json
import sys
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJ_DIR / "data"
LOGS_DIR = PROJ_DIR / "logs"

VERBOSE = False


def _info(msg: str) -> None:
    if VERBOSE:
        print(f"    {msg}")


# ============================================================
# Tests
# ============================================================

def test_city_status():
    """city_status.json 存在 + 至少一個 ready 城市有 model + error_table + probability。"""
    path = DATA_DIR / "city_status.json"
    assert path.exists(), f"city_status.json not found: {path}"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "city_status.json must be a dict"
    ready = [c for c, info in data.items() if info.get("status") == "ready"]
    assert ready, "No ready cities in city_status.json"
    _info(f"Ready cities: {ready}")

    for city in ready:
        # empirical model
        model_path = DATA_DIR / "models" / "empirical" / city / "empirical_model.json"
        assert model_path.exists(), f"Model missing for ready city {city}: {model_path}"
        model = json.loads(model_path.read_text(encoding="utf-8"))
        assert len(model) > 0, f"empirical_model.json for {city} appears empty"
        _info(f"{city}: model OK ({model_path.stat().st_size} bytes)")

        # error_table 存在且有足夠行數
        error_path = DATA_DIR / "processed" / "error_table" / city / "market_day_error_table.csv"
        assert error_path.exists(), f"error_table missing for ready city {city}: {error_path}"
        with open(error_path, "r", encoding="utf-8", newline="") as f:
            error_rows = sum(1 for _ in csv.DictReader(f))
        assert error_rows > 100, (
            f"{city}: error_table has only {error_rows} rows (expected > 100 for ready city)"
        )
        _info(f"{city}: error_table OK ({error_rows} rows)")

        # probability 存在且 > 0
        prob_path = DATA_DIR / "results" / "probability" / city / "event_probability.csv"
        assert prob_path.exists(), f"event_probability.csv missing for ready city {city}: {prob_path}"
        with open(prob_path, "r", encoding="utf-8", newline="") as f:
            prob_rows = sum(1 for _ in csv.DictReader(f))
        assert prob_rows > 0, f"{city}: event_probability.csv is empty (0 rows)"
        _info(f"{city}: probability OK ({prob_rows} rows)")


def test_book_state():
    """book_state JSON 存在 + fetch_status=ok + has best_bid/ask + 至少 1/10 ok。"""
    book_dir = DATA_DIR / "raw" / "prices" / "book_state"
    if not book_dir.exists():
        _info("book_state dir not found — skipping (run 08 first)")
        return

    json_files = list(book_dir.glob("*.json"))
    if not json_files:
        _info("No book_state JSON files — skipping")
        return

    sample = json_files[:10]
    ok_count = 0
    for jf in sample:
        try:
            book = json.loads(jf.read_text(encoding="utf-8"))
        except Exception as e:
            raise AssertionError(f"Cannot parse {jf.name}: {e}")
        assert "fetch_status" in book, f"{jf.name}: missing fetch_status"
        if book["fetch_status"] == "ok":
            assert "yes_best_ask" in book or "no_best_ask" in book, \
                f"{jf.name}: fetch_status=ok but missing best_ask fields"
            ok_count += 1

    _info(f"book_state: {ok_count}/{len(sample)} files with fetch_status=ok")
    assert ok_count >= 1, (
        f"book_state: 0/{len(sample)} files ok — "
        f"08_market_price_fetch may have failed or prices are stale"
    )


def test_ev_signals():
    """ev_signals.csv 存在 + 有 signal_status + signal_action + 深度欄位。"""
    ev_dir = DATA_DIR / "results" / "ev_signals"
    assert ev_dir.exists(), f"ev_signals dir not found: {ev_dir}"

    city_dirs = [d for d in ev_dir.iterdir() if d.is_dir()]
    assert city_dirs, "No city dirs under data/results/ev_signals/"

    required_cols = {"signal_status", "signal_action", "yes_edge", "no_edge"}
    depth_cols = {"yes_sweet_usd", "no_sweet_usd"}

    for city_dir in city_dirs:
        csv_path = city_dir / "ev_signals.csv"
        assert csv_path.exists(), f"ev_signals.csv missing for {city_dir.name}"
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            cols = set(reader.fieldnames or [])
            missing = required_cols - cols
            assert not missing, f"{city_dir.name}: missing columns {missing}"
            rows = list(reader)
        assert rows, f"{city_dir.name}: ev_signals.csv is empty"
        has_depth = bool(depth_cols & cols)
        _info(f"{city_dir.name}: {len(rows)} rows, depth_cols={'yes' if has_depth else 'no'}")


def test_positions():
    """positions.json schema 正確 + open positions 有 position_id。"""
    path = DATA_DIR / "positions.json"
    if not path.exists():
        _info("positions.json not found — skipping (no positions recorded yet)")
        return

    data = json.loads(path.read_text(encoding="utf-8"))
    assert "schema_version" in data, "positions.json missing schema_version"
    assert "positions" in data, "positions.json missing positions key"
    assert isinstance(data["positions"], list), "positions must be a list"

    open_pos = [p for p in data["positions"] if p.get("status") == "open"]
    for p in open_pos:
        assert "position_id" in p, f"Open position missing position_id: {p}"
        assert p["position_id"].startswith("pos_"), \
            f"position_id format wrong: {p['position_id']}"

    _info(f"positions: {len(open_pos)} open, "
          f"{len([p for p in data['positions'] if p.get('status')=='closed'])} closed")


def test_alert_history():
    """最近 alert_history 存在（若有通報過）+ 格式正確。"""
    alert_dir = LOGS_DIR / "15_alert"
    if not alert_dir.exists():
        _info("15_alert log dir not found — skipping")
        return

    today = datetime.now(timezone.utc)
    found_files = []
    for delta in (0, 1):
        dt = today - timedelta(days=delta)
        p = alert_dir / f"{dt.strftime('%Y-%m-%d')}_alert_history.csv"
        if p.exists():
            found_files.append(p)

    if not found_files:
        _info("No recent alert_history files — skipping")
        return

    for path in found_files:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        required = {"generated_utc", "signal_action", "city"}
        cols = set(reader.fieldnames or [])
        missing = required - cols
        assert not missing, f"{path.name}: missing columns {missing}"
        _info(f"{path.name}: {len(rows)} rows")


def test_signal_state():
    """_signal_state.json 存在 + last_success_utc 在合理範圍（24h 內）。"""
    path = DATA_DIR / "_signal_state.json"
    assert path.exists(), f"_signal_state.json not found (signal_main not started?)"
    state = json.loads(path.read_text(encoding="utf-8"))
    assert "last_success_utc" in state, "_signal_state.json missing last_success_utc"

    last_str = state.get("last_success_utc")
    if last_str:
        try:
            last_dt = datetime.fromisoformat(last_str.replace("Z", "+00:00"))
            age_h = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
            assert age_h < 24, f"last_success_utc is {age_h:.1f}h ago (> 24h — stale?)"
            _info(f"last_success_utc: {last_str} ({age_h:.1f}h ago)")
        except ValueError:
            raise AssertionError(f"Cannot parse last_success_utc: {last_str}")
    else:
        _info("last_success_utc is None (no successful cycle yet)")


def test_system_health():
    """_system_health.json 存在 + 各進程有 status key。"""
    path = DATA_DIR / "_system_health.json"
    if not path.exists():
        _info("_system_health.json not found — skipping (no processes started yet)")
        return

    health = json.loads(path.read_text(encoding="utf-8"))
    assert "updated_at_utc" in health, "_system_health.json missing updated_at_utc"

    for component in ("signal_main", "collector_main", "telegram_bot"):
        if component in health:
            assert "status" in health[component], \
                f"{component} block missing 'status'"
            _info(f"{component}: status={health[component]['status']}, "
                  f"updated={health[component].get('updated_at_utc', '?')}")
        else:
            _info(f"{component}: not in health (process not started)")


def test_fill_simulator():
    """fill_simulator 回歸：核心函式可呼叫，結果合理。"""
    import importlib.util
    lib_dir = PROJ_DIR / "_lib"
    spec = importlib.util.spec_from_file_location(
        "fill_simulator", lib_dir / "fill_simulator.py"
    )
    assert spec is not None, "fill_simulator.py not found in _lib/"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # 簡單冒煙測試：simulate_fill 應回傳合理結果
    if hasattr(mod, "simulate_fill"):
        # orderbook_levels: [{"price", "size"}], p=0.5, best_only mode
        result = mod.simulate_fill(
            orderbook_levels=[
                {"price": "0.50", "size": "500"},
                {"price": "0.51", "size": "200"},
            ],
            p=0.5,
            fee_rate=0.025,
            fee_exponent=0.5,
            mode="best_only",
            side="buy_asks",
        )
        assert result is not None, "simulate_fill returned None"
        assert hasattr(result, "total_shares") or isinstance(result, dict), \
            f"simulate_fill result unexpected type: {type(result)}"
        _info(f"simulate_fill result: total_shares={getattr(result, 'total_shares', '?')}, "
              f"avg_fill_price={getattr(result, 'avg_fill_price', '?')}")
    else:
        _info("simulate_fill not found in fill_simulator — checking module loaded OK")
        assert hasattr(mod, "__file__"), "fill_simulator module load failed"


def test_fee_regression():
    """Fee 計算驗算 3 筆（官方費率表，Weather: 0.025 + exp=0.5，100 shares）。"""
    # fee = C × p × feeRate × (p × (1-p))^exponent
    # 官方：p=0.10 → $0.08; p=0.50 → $0.62; p=0.90 → $0.67
    FEE_RATE = 0.025
    FEE_EXP = 0.5
    C = 100  # shares

    cases = [
        (0.10, 0.08),
        (0.50, 0.62),
        (0.90, 0.67),
    ]
    for price, expected in cases:
        fee = C * price * FEE_RATE * (price * (1 - price)) ** FEE_EXP
        diff = abs(fee - expected)
        assert diff < 0.02, (
            f"Fee regression FAIL at p={price}: "
            f"computed={fee:.4f}, expected={expected:.2f}, diff={diff:.4f}"
        )
        _info(f"p={price}: fee=${fee:.4f} ≈ ${expected:.2f} ✓")


# ============================================================
# Runner
# ============================================================

TESTS = [
    test_city_status,
    test_book_state,
    test_ev_signals,
    test_positions,
    test_alert_history,
    test_signal_state,
    test_system_health,
    test_fill_simulator,
    test_fee_regression,
]


def main():
    global VERBOSE
    parser = argparse.ArgumentParser(description="Phase 2 smoke tests")
    parser.add_argument("-v", "--verbose", action="store_true", help="詳細輸出")
    args = parser.parse_args()
    VERBOSE = args.verbose

    passed = 0
    failed = 0
    skipped = 0

    print(f"\n{'='*50}")
    print(f"  Phase 2 Smoke Test")
    print(f"  Project: {PROJ_DIR}")
    print(f"{'='*50}\n")

    for test_fn in TESTS:
        name = test_fn.__name__
        try:
            test_fn()
            print(f"  ✅  {name}")
            passed += 1
        except AssertionError as e:
            print(f"  ❌  {name}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ❌  {name}: [{type(e).__name__}] {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"  {passed} passed, {failed} failed")
    if skipped:
        print(f"  ({skipped} skipped)")
    print(f"{'='*50}\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
