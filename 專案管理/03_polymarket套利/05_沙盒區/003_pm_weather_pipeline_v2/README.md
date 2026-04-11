# PM Weather Signal Pipeline

Polymarket 天氣合約交易信號系統。用 GFS 預報誤差分布（ECDF）計算事件機率，與市場價格比較找出 edge，透過 Telegram Bot 推送信號。

---

## 系統架構

```
collector_main（排程 daemon）
├── 每 6h：GFS forecast 更新（05 → GFS peak → 07 → 09 → 10）
├── 每 24h：WU truth 更新（06 → 07 → 09 → 10）
├── 每日：城市掃描 + 回補（12 → 13 → 14）
└── obs-fetch（獨立 daemon thread，每 10min）
    → WU current → latest_obs.json + current_obs_YYYY-MM.csv

signal_main（信號 daemon，每 30 秒）
├── 抓報價（08）
├── 讀 latest_obs.json（不直接打 WU）
├── 跑 11_ev_engine.py（即時 ECDF + remaining_gain + 三模式信號）
└── 跑 15_alert_engine.py

telegram_bot（唯讀 UI daemon）
├── 讀 per-city ev_signals.csv
├── 讀 latest_obs.json
└── 讀 GFS peak / static peak fallback
```

---

## 核心流程（白話版）

1. **收集**：每 6 小時去問氣象局（GFS）未來 7 天每小時幾度；每天確認昨天真正幾度；每 10 分鐘看現在幾度
2. **建模**：用 800 多天的「預測 vs 實際」差值，建一本歷史答案本（ECDF 模型）
3. **算機率**：每 30 秒翻答案本，根據「離結算還多久」查對應那頁的機率分布
4. **比價格**：拿機率跟 Polymarket 的市場價格比，找到市場定價錯誤的合約
5. **推送**：把有 edge 的信號推到 Telegram，操盤手決定是否下單

---

## Pipeline

### 核心資料鏈

| 步驟 | 腳本 | 輸入 | 輸出 |
|------|------|------|------|
| 1 | `03_market_catalog.py` | Polymarket Gamma API | `data/03_market_catalog.csv` |
| 2 | `04_market_master.py` | 03 CSV + seed_cities + override | `data/market_master.csv` |
| 3 | `05_D_forecast_fetch.py` | Open-Meteo GFS API | `data/raw/D/{city}/gfs_seamless/forecast_hourly_*.csv` |
| 4 | `06_B_truth_fetch.py` | Weather Underground | `data/raw/B/{city}/truth_daily_high.csv` |
| 5 | `07_daily_high_pipeline.py` | raw forecast + truth | `forecast_daily_high` + `truth_daily_high_b` + `error_table` |
| 6 | `09_model_engine.py` | error_table | `data/models/empirical/{city}/empirical_model.json` |
| 7 | `10_event_probability.py` | empirical model + forecast + market_master | `data/results/probability/{city}/event_probability.csv` |
| 8 | `08_market_price_fetch.py` | Polymarket CLOB API | `market_prices.csv` + `book_state/*.json` |
| 9 | `11_ev_engine.py` | probability + prices + params + obs + RG model | `data/results/ev_signals/{city}/ev_signals.csv` |

### 城市 Onboarding

```
12_city_scanner.py → 13_city_status_manager.py → 14_backfill_manager.py
```

狀態流：`discovered → backfilling → ready → failed / disabled`

回補流程：`05 → 06 → 07 → 09 → 03 → 04 → 10`。03/04 為 blocking，10 在 `--cities` 模式下啟用 strict_mode。

---

## 機率模型

### Empirical ECDF（核心）

誤差定義：`error = actual_daily_high_c - predicted_daily_high_c`

Bucket 結構：`lead_hours_X`（每 6h，≥100 筆）為主，`lead_day_X`（每天，≥5 筆）為 fallback。

插值：相鄰 bucket 之間用 quantile interpolation（1D Wasserstein barycenter）平滑。三層 hierarchy：lead_hours 插值 → lead_day 插值 → nearest fallback。

事件邊界（閉區間）：

```
exact X:   actual ∈ [X-h, X+h]     h = precision_half
range L-H: actual ∈ [L-h, H+h]
higher X:  actual ≥ X-h
below X:   actual < X+h
```

內部單位：predicted_daily_high 和 model errors 永遠是 °C。華氏市場的門檻先轉 °C 再計算。

### 即時機率（Phase 1.5）

11_ev_engine.py 支援三種機率來源，優先序：

| `probability_mode` | 條件 | 說明 |
|--------------------|------|------|
| `remaining_gain` | ≤6h + 新鮮觀測 | 觀測 running max + 歷史剩餘升幅 ECDF |
| `realtime_ecdf` | `use_realtime_probability: true` | 每 30 秒用當前 lead_hours 即時查 ECDF |
| `batch_ecdf` | fallback | 10 批次寫好的固定 p_yes |

11 使用 module-level lazy cache + mtime reload 載入 empirical model。

### Remaining Gain（≤6h）

`remaining_gain = final_max - running_max`，按 local_hour 分 24 桶。`tools/build_remaining_gain.py` 建模。

---

## 三種信號模式

| 模式 | 邏輯 | 信號量 |
|------|------|--------|
| 散彈（scatter） | EV > 0 就出信號 | 最多 |
| 精準（precision） | 散彈 + 90% 區間方向矛盾時壓制 | 中等 |
| 點射（sniper） | 90% 區間必須完全落在信號方向一側 | 最少 |

lock_range 用 signed error p05/p95（非對稱）。higher/below 可方向鎖定，exact/range 只做逆風壓制。

---

## GFS 動態峰值

`_lib/gfs_peak_hours.py` 從 GFS hourly 找每日最高溫時段（±1h 窗口）。先選最新 snapshot → 該 snapshot 的 24 筆 → `forecast_temp` argmax。

峰值來源優先序：`gfs_peak_hours.json` → `city_peak_hours.json`（靜態）。Google Weather API 已凍結。

峰值已過自動鎖定（`now_local > peak_end_local_datetime`）。

---

## 即時觀測

collector obs thread 每 10 分鐘寫 `latest_obs.json`。signal_main 只讀此檔（fail-open：缺檔 = 不裁剪）。

Observation clipping：用即時觀測做物理邏輯裁剪（最終最高溫 ≥ 目前最高溫）。統一使用 `parse_obs_time_utc()` 解析時間。

---

## 安全閘門

| 條件 | 行為 |
|------|------|
| 結算 < 6h（非 RG） | SUPPRESSED |
| 結算 < 6h（RG 模式） | 正常（RG 目標就是此窗口） |
| 結算 6-8h | 標記 last_forecast_warning |
| 一邊 > 95¢ | SUPPRESSED |
| 無掛單 / 報價超齡 / Book 不完整 | SUPPRESSED |

---

## Telegram Bot

主鍵盤：`[ 今日 ] [ 預警6-8h ] [ 結算<6h ]` / `[ 排行 ] [ 城市 ] [ 管理 ]`

管理面板頂部：模式切換 `[ 散彈 ] [ 精準 ] [ 點射 ] [ 說明 ]`（所有 allowed 用戶可見）

峰值顯示：倒數中 / 🔥峰值進行中 / ✅峰值已過

所有時間顯示為台北時間（UTC+8）。

---

## Config

| 檔案 | 用途 |
|------|------|
| `config/trading_params.yaml` | 所有可調參數 |
| `config/seed_cities.json` | 城市 metadata |
| `config/city_override.json` | 覆蓋 seed |
| `config/telegram.yaml` | Bot token（不進 git） |
| `config/wu_api_key.txt` | WU Key（不進 git） |
| `config/city_peak_hours.json` | 靜態峰值 fallback |

```yaml
# trading_params.yaml 重要旗標
use_realtime_probability: true
use_convergence_interpolation: true
remaining_gain_enabled: true
signal_mode: scatter
direction_lock_confidence: 0.90
```

---

## 資料目錄

```
data/
├── market_master.csv
├── city_status.json / users.json
├── gfs_peak_hours.json                    # GFS 動態峰值
├── observations/latest_obs.json           # 即時觀測
├── raw/D/{city}/gfs_seamless/             # GFS 預報
├── raw/B/{city}/                          # WU 真值
├── raw/prices/                            # 報價 + orderbook
├── processed/error_table/{city}/          # 預報誤差表
├── models/empirical/{city}/               # ECDF 模型
├── models/remaining_gain/{city}/          # RG 模型
├── results/probability/{city}/            # 批次機率
└── results/ev_signals/{city}/             # EV 信號（Bot 主讀）
```

---

## 部署

```bash
pip install requests pyyaml "python-telegram-bot>=20"
python 02_init.py

screen -S collector -dm bash -c "cd /opt/pm-weather && python3 collector_main.py --verbose"
screen -S signal -dm bash -c "cd /opt/pm-weather && python3 signal_main.py --mode rest --interval 30 --verbose"
screen -S bot -dm bash -c "cd /opt/pm-weather && python3 telegram_bot.py"
```

安全重啟用 `screen -S {name} -X quit`，不要 `killall screen`。

---

## 腳本清單

| 檔案 | 功能 |
|------|------|
| `03_market_catalog.py` | 市場掃描 |
| `04_market_master.py` | 交易主表 |
| `05_D_forecast_fetch.py` | GFS 預報抓取 |
| `06_B_truth_fetch.py` | WU 真值抓取 |
| `07_daily_high_pipeline.py` | daily high + 誤差表 |
| `08_market_price_fetch.py` | 報價抓取 |
| `09_model_engine.py` | ECDF 建模 |
| `10_event_probability.py` | 批次機率 + 插值 + q05/q95 |
| `11_ev_engine.py` | 即時 p_yes + EV + 三模式 + RG |
| `12-14` | 城市掃描 / 狀態機 / 回補 |
| `15_alert_engine.py` | Alert 推送 |
| `20_backtest.py` | 三模式回測 |
| `_lib/ecdf_query.py` | ECDF 查詢核心 |
| `_lib/gfs_peak_hours.py` | GFS 峰值計算 |
| `_lib/signal_reader.py` | Bot 讀取層（mode-aware） |
| `_lib/obs_time_utils.py` | obs 時間解析 |
| `tools/build_remaining_gain.py` | RG 模型建檔 |
| `tools/build_peak_hours.py` | 靜態峰值建檔 |