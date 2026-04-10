# PM Weather Signal Pipeline v7.2

Polymarket 天氣合約交易信號系統。用 GFS 預報誤差分布（ECDF）計算事件機率，與市場價格比較找出 edge，透過 Telegram Bot 推送信號。

---

## 系統架構

```
collector_main（排程 daemon）
  ├── 每 6h：GFS 預報更新（05→07→09→10）
  ├── 每 24h：WU 真值更新（06→07→09→10）
  ├── 每 24h：城市掃描 + 新城市回補（12→13→14）
  └── 每 10min：即時觀測抓取 → latest_obs.json + CSV

signal_main（信號 daemon，每 30 秒）
  ├── 抓報價（08）
  ├── 跑 EV 計算（11）
  ├── 寫 signal_summary.json
  └── alert 推送（15）

telegram_bot（唯讀 UI daemon）
  └── 讀 signal_summary.json + latest_obs.json → 顯示頁面
```

---

## Pipeline 流程

### 核心資料鏈

| 步驟 | 腳本 | 輸入 | 輸出 |
|------|------|------|------|
| 1 | `03_market_catalog.py` | Polymarket Gamma API | `data/03_market_catalog.csv` |
| 2 | `04_market_master.py` | 03 CSV + seed_cities + override | `data/market_master.csv` |
| 3 | `05_D_forecast_fetch.py` | Open-Meteo GFS API | `data/raw/D/{city}/gfs_seamless/forecast_hourly_*.csv` |
| 4 | `06_B_truth_fetch.py` | Weather Underground | `data/raw/B/{city}/truth_daily_high.csv` |
| 5 | `07_daily_high_pipeline.py` | raw forecast + truth | `forecast_daily_high` + `truth_daily_high_b` + `error_table` |
| 6 | `09_model_engine.py` | error_table | `data/models/empirical/{city}/empirical_model.json` |
| 7 | `10_event_probability.py` | model + forecast + market_master | `data/results/probability/{city}/event_probability.csv` |
| 8 | `08_market_price_fetch.py` | Polymarket CLOB API | `market_prices.csv` + `book_state/*.json` |
| 9 | `11_ev_engine.py` | probability + prices + trading_params | `data/results/ev_signals/{city}/ev_signals.csv` |

### 城市 Onboarding

```
12_city_scanner.py → 13_city_status_manager.py → 14_backfill_manager.py
  （掃描市場）         （狀態機管理）              （回補 05→06→07→09→03→04→10）
```

---

## 常駐服務

### collector_main.py

```bash
python collector_main.py --verbose      # 常駐
python collector_main.py --once         # 一次性
python collector_main.py --once-obs     # 只跑一次觀測
```

| 任務 | 週期 |
|------|------|
| 城市掃描（12→13→14） | 每日 06:00 UTC |
| Truth 更新（06→07→09→10） | 每日 00:00 UTC |
| Forecast 更新（05→07→09→10） | 每 6 小時 |
| 即時觀測（WU current） | 每 10 分鐘 |

### signal_main.py

```bash
python signal_main.py --mode rest --interval 30 --verbose   # REST 模式
python signal_main.py --mode ws --verbose                   # WebSocket 模式
python signal_main.py --once                                # 一次性
```

每輪：讀 ready 城市 → 08 抓報價 → 11 EV 計算 → 寫 signal_summary.json → 15 alert 推送

容錯：連續失敗 ≥3 次 → sleep 60 秒

### telegram_bot.py

```bash
python telegram_bot.py
```

純唯讀 UI，不直接呼叫 API 或修改資料。讀 signal_summary.json 和 latest_obs.json。

---

## Telegram Bot 頁面

### 按鈕

```
[ 今日 ] [ 預警6-8h ] [ 結算<6h ]
[ 排行 ] [   城市    ] [  管理   ]
```

### 頁面規格

| 頁面 | 時間範圍 | 篩選 | 特色 |
|------|---------|------|------|
| 排行 | > 24h | BUY 信號（含幾乎確定） | 按 edge 或 depth 排序 |
| 今日 | 8-24h | BUY 信號（含幾乎確定） | 加預報+實況溫度+峰值時段 |
| 預警 | 6-8h | BUY 信號（含幾乎確定） | GFS 最後一版預報 |
| 結算中 | < 6h | 全部合約 | 已鎖定/未鎖定分類 |
| 城市 | 全部 | 全部合約 | 溫度 ladder + 日期按鈕 |
| 管理 | — | — | 用戶管理 + 系統狀態 |

### 城市頁 6 種狀態

| 條件 | 顯示 |
|------|------|
| edge > 0 | `YES▲+4.2%` 或 `NO▲+12.2%` |
| edge ≤ 0 | `無優勢` |
| observation_clipped | `已鎖定（已超過）` |
| market_extreme + NO 貴 | `幾乎確定NO` |
| market_extreme + YES 貴 | `幾乎確定YES` |
| 無掛單 | `無掛單` |
| 價格過時 | `價格過時` |

### 時間顯示

所有時間顯示為台北時間（UTC+8）。

---

## 模型

### Empirical ECDF（核心模型）

用 GFS 預報誤差（actual - predicted）的歷史分布，按 lead time 分 bucket，計算每個合約的 p_yes / p_no。

**Bucket 結構**：

| 類型 | 粒度 | 最低樣本量 | 說明 |
|------|------|-----------|------|
| `lead_hours_X` | 每 6 小時 | ≥ 100 | 主要使用 |
| `lead_day_X` | 每天 | ≥ 5 | fallback |

**Bucket 選擇優先序**：lead_hours exact → lead_day exact → 最近 lead_day fallback

**事件邊界（半開區間）**：

```
exact X:   actual ∈ [X-h, X+h)     h = precision_half
range L-H: actual ∈ [L-h, H+h)
higher X:  actual ≥ X-h
below X:   actual < X+h
```

### OU/AR(1)（可選）

`data/models/ou_ar/{city}/ou_model.json`，09 best-effort 產出，10 目前未使用。

### Quantile Regression（可選）

需要 `statsmodels`，09 best-effort 產出，10 目前未使用。

---

## 即時觀測

```
collector_main（每 10 分鐘）
  → WU v3 API（by ICAO station code）
  → data/observations/latest_obs.json（merge 式，原子寫入）
  → data/observations/current_obs_YYYY-MM.csv（按月分檔，append）

signal_main → 讀 latest_obs.json → 傳給 11（觀測裁剪）
telegram_bot → 讀 latest_obs.json → 顯示實況溫度
```

**latest_obs.json schema**：

```json
{
  "schema_version": 1,
  "updated_at_utc": "2026-04-10T14:31:05Z",
  "cities": {
    "London": {
      "high_c": 22.0,
      "current_temp_c": 18.5,
      "obs_time_utc": "2026-04-10T14:30:00Z",
      "fetched_at_utc": "2026-04-10T14:31:05Z",
      "source": "v3_current",
      "station_code": "EGLC",
      "status": "ok",
      "schema_version": 1
    }
  }
}
```

只抓 ready 城市。抓取失敗時保留舊值，不覆蓋。

---

## 安全閘門

| 條件 | signal_status | signal_action | Bot 顯示 |
|------|-------------|---------------|---------|
| 結算 < 6h | too_close_to_settlement | SUPPRESSED | 結算中頁 |
| 結算 6-8h | last_forecast_warning | 正常判斷 | 預警頁 |
| 一邊 > 95¢ | market_extreme | 正常判斷 | 幾乎確定YES/NO |
| 無掛單 | no_price | SUPPRESSED | 無掛單 |
| 報價超齡 | stale_price | SUPPRESSED | 價格過時 |
| 正常 | active | BUY_YES/BUY_NO/NO_TRADE | 排行/今日 |

---

## 資料目錄

```
data/
├── market_master.csv                    # 交易主表
├── city_status.json                     # 城市狀態機
├── 03_market_catalog.csv                # 市場清單
├── positions.json                       # 持倉記錄
├── _signal_state.json                   # signal 迴圈狀態
├── _system_health.json                  # 進程心跳
├── observations/
│   ├── latest_obs.json                  # 即時觀測快照
│   └── current_obs_YYYY-MM.csv          # 觀測歷史（按月）
├── raw/
│   ├── D/{city}/gfs_seamless/           # GFS 逐小時預報
│   ├── B/{city}/                        # WU 真值
│   └── prices/                          # 市場報價 + orderbook
├── processed/
│   ├── forecast_daily_high/{city}/      # 每日最高溫預報
│   ├── truth_daily_high/                # 真值彙整
│   └── error_table/{city}/              # 預報誤差表
├── models/
│   ├── empirical/{city}/                # ECDF 模型（核心）
│   ├── ou_ar/{city}/                    # OU/AR(1)（可選）
│   └── quantile_regression/{city}/      # QR（可選）
└── results/
    ├── probability/{city}/              # 事件機率
    ├── ev_signals/{city}/               # EV 信號
    └── signal_summary.json              # 預計算信號分組
```

---

## Config

| 檔案 | 用途 | 注意 |
|------|------|------|
| `config/seed_cities.json` | 城市 metadata（station_code / timezone / unit 等） | |
| `config/city_override.json` | 覆蓋 seed_cities（優先序最高） | |
| `config/trading_params.yaml` | 交易參數（fee / edge / 安全閘門 / TTL） | |
| `config/telegram.yaml` | Bot token + chat ID | **含 secret，不進 git** |
| `config/wu_api_key.txt` | WU API Key | **含 secret，不進 git** |
| `config/city_peak_hours.json` | 各城市各月峰值時段 | `tools/build_peak_hours.py` 生成 |

---

## 部署

### 環境

```bash
# Python 3.9+（建議 3.10+）
pip install requests pyyaml "python-telegram-bot>=20"

# 可選
pip install websockets              # WS 價格流
pip install numpy statsmodels       # QR 模型

# 環境檢查
python 02_init.py
```

### 啟動（三進程）

```bash
screen -S collector -dm bash -c "cd /opt/pm-weather && python3 collector_main.py --verbose"
screen -S signal -dm bash -c "cd /opt/pm-weather && python3 signal_main.py --mode rest --interval 30 --verbose"
screen -S bot -dm bash -c "cd /opt/pm-weather && python3 telegram_bot.py"
```

### 維運

```bash
screen -r collector          # 接入 log（Ctrl+A,D 分離）
screen -ls                   # 列出所有 screen

# 城市管理
python3 13_city_status_manager.py --list
python3 13_city_status_manager.py --ready

# 手動回補
python3 14_backfill_manager.py --cities "Tokyo,Seoul"
python3 14_backfill_manager.py --retry-failed

# 測試
python3 tools/smoke_test.py -v
```

### 更新

```bash
git pull origin main
killall screen
# 重新啟動三進程
```

---

## 腳本清單

| 檔案 | 功能 |
|------|------|
| `01_main.py` | 手動 dispatcher（串跑 03→11） |
| `02_init.py` | 目錄初始化 + 環境檢查 |
| `03_market_catalog.py` | Gamma API 掃描市場 + 語義解析 |
| `04_market_master.py` | 合併 catalog + seed → 交易主表 |
| `05_D_forecast_fetch.py` | GFS 預報抓取（live / historical） |
| `06_B_truth_fetch.py` | WU 真值抓取 |
| `07_daily_high_pipeline.py` | hourly → daily + 誤差表 |
| `08_market_price_fetch.py` | Polymarket CLOB REST 報價 |
| `08b_price_stream.py` | WebSocket 價格流 |
| `08c_book_state.py` | In-memory orderbook 管理 |
| `09_model_engine.py` | 誤差分布建模（ECDF / OU / QR） |
| `10_event_probability.py` | ECDF 計算 p_yes / p_no |
| `11_ev_engine.py` | EV + edge + 安全閘門 + 信號 |
| `12_city_scanner.py` | 掃描可用城市 |
| `13_city_status_manager.py` | 城市狀態機 |
| `14_backfill_manager.py` | 新城市回補 |
| `15_alert_engine.py` | 信號篩選 + Telegram 推送 |
| `16_position_manager.py` | 持倉追蹤 |
| `collector_main.py` | 排程 daemon |
| `signal_main.py` | 信號 daemon |
| `telegram_bot.py` | Telegram Bot UI |
| `_lib/signal_reader.py` | Bot 資料讀取層 |
| `_lib/current_obs_fetcher.py` | WU 即時觀測 fetcher |
| `_lib/fill_simulator.py` | Orderbook 填充模擬 |
| `tools/smoke_test.py` | 回歸測試 |
| `tools/build_peak_hours.py` | 峰值時段建檔 |

`legacy/` 和 `_lib_legacy/` 為歷史封存，不屬於主線。