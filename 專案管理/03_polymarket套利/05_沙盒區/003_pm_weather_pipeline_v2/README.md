# Polymarket Weather EV Pipeline

Polymarket 天氣市場 EV 信號產生器。自動抓取 GFS 氣象預報、歷史觀測真值、市場即時報價，用統計模型找出具有正期望值的交易機會。

**狀態：v6.3.0（STEP 1–14A 全部通過 + Bot UX 重設計 + Admin 面板 + 防禦性 Bug 修正 20 項 + Backfill 逐城市完整鏈路）**
**定位：D1-only MVP，London + Paris 自動偵測 ready，可擴展至 40+ 城市。僅供管線驗證，不可用於實際交易決策。**

---

## 核心邏輯（白話版）

**第一步**：從 Open-Meteo 抓 GFS 數值天氣模型的預測，知道「4 天後 London 預測最高溫是 17.7°C」。

**第二步**：累積過去 800 天「GFS 預測 vs 實際最高溫」的誤差，例如「D4 提前預測通常偏差 -1°C ~ +3°C」，建立一個誤差分布模型（ECDF）。

**第三步**：把「預測溫度 + 每一個歷史誤差」= 一個可能的「實際溫度」。計算其中有多少比例落在合約指定的溫度區間，得到模型估計的 p(YES)。

**第四步**：抓 Polymarket 市場現在的 YES/NO 報價，與模型估計的 p(YES) 對比。若市場低估了 YES（或 NO），計算出統計 edge 和 Kelly 建議下注金額，輸出交易信號。

---

## Pipeline 架構

```
Polymarket Gamma API
        ↓
03_market_catalog.py  → 解析市場清單 → data/03_market_catalog.csv
        ↓
04_market_master.py   → 補站點資料   → data/market_master.csv
        ↓
05_D_forecast_fetch.py → 抓 GFS 預報 → data/raw/D/{city}/gfs_seamless/forecast_hourly_{date}.csv
06_B_truth_fetch.py    → 抓 WU 真值  → data/raw/B/{city}/truth_daily_high.csv
        ↓
07_daily_high_pipeline.py
  Step A: 原始逐時預報 → 每日最高溫快照 (表2)
  Step B: 原始真值     → 去重規範表 (表3)
  Step C: 表2 + 表3   → 誤差表 (表4)
        ↓
09_model_engine.py  → 建模 → data/models/empirical/{city}/empirical_model.json
        ↓
10_event_probability.py → 計算 p(YES) → data/results/probability/{city}/event_probability.csv
        ↓
08_market_price_fetch.py → 抓 CLOB 報價 → data/raw/prices/market_prices.csv
        ↓
11_ev_engine.py → EV + signal → data/results/ev_signals/{city}/ev_signals.csv
```

**長駐進程（三個獨立 process）：**

```
collector_main.py   → 每小時/天更新 forecast + truth + 模型 + 城市掃描
signal_main.py      → 每 5 分鐘更新報價 + EV + 通報
telegram_bot.py     → Bot UI（只讀 finalized outputs）
```

---

## Telegram Bot UI

### 啟動方式

```bash
python telegram_bot.py
```

### Reply Keyboard（所有授權用戶）

```
[ 排行 ] [ 城市 ] [ 管理 ]
```

### 排行頁（排行按鈕）

顯示 Edge 或 Depth 前 N 名，每頁 5 筆：

```
價差排名

1 · Paris · 04/09 · 20小時
    23°C NO
    $0.008 → $0.010  +20.5%  $71

2 · London · 04/11 · 2天20小時
    18°C YES
    $0.120 → $0.139  +15.7%  $22

                          1 / 3
```

- 城市和溫度粗體
- 進場→目標價用箭頭
- 結算時間（X天Y小時，0天省略）
- 排序按鈕：價差 / 深度

### 城市議題頁（城市按鈕 → 選城市）

顯示單一城市單日所有合約：

```
Paris — 04/09
結算：20小時

23°C NO
$0.008 → $0.010  +20.5%  $71

22°C YES
$0.009 → $0.010  +7.4%  $18
```

- 結算只顯示一次
- 不分頁，全部顯示
- 過濾 SUPPRESSED / 進場價=0 / 深度=0
- 按 Edge 由高到低排
- ← → 翻日期

### 管理面板（管理按鈕）

**Admin 看到：**
- 顯示我的 ID
- 新增用戶 / 刪除用戶
- 所有用戶
- 系統狀態

**一般用戶看到：**
- 顯示我的 ID

### 未授權用戶

任何操作都顯示：

```
請找管理員授權

你的 ID：123456789
請將此 ID 傳給管理員
```

### 隱藏功能（程式碼已實作，UI 未開放）

- 持倉追蹤（positions）
- 記錄進場 / 平倉
- Alert push 按鈕
- Edge 縮水推送
- 通報歷史
- 設定頁面

---

## 部署（三個進程）

目前部署於阿里雲香港 VPS，用 GNU screen 管理三個常駐進程。

```bash
# 進程 1：Collector（forecast/truth/model 更新 + 城市掃描 + backfill）
screen -S collector
python collector_main.py

# 進程 2：Signal（報價 + EV + 通報）
screen -S signal
python signal_main.py

# 進程 3：Bot UI
screen -S bot
python telegram_bot.py
```

### 常用維運指令

```bash
# 查看所有 screen
screen -ls

# 接回指定進程
screen -r collector
screen -r signal
screen -r bot

# 系統狀態
python tools/smoke_test.py -v

# 手動回補城市
python 14_backfill_manager.py --cities "Tokyo"

# 手動重跑 failed 城市
python 14_backfill_manager.py --retry-failed

# 查看城市狀態
python 13_city_status_manager.py --list
python 13_city_status_manager.py --ready
```

### 排程

| 任務 | 時間（UTC） |
|------|------------|
| truth 更新 | 00:00 |
| 城市掃描 | 06:00 |
| forecast 更新 | 每 6 小時 |
| backfill（discovered 城市） | 每輪 scan 後自動觸發 |
| failed 城市 auto-retry | 每次城市掃描成功後自動重置 |

---

## Config

```
config/
├── telegram.yaml          # Bot token + chat_id（不進 git）
├── telegram.yaml.example  # 佔位符範本（進 git）
├── trading_params.yaml    # 交易參數（進 git）
├── seed_cities.json       # 城市種子清單
└── city_override.json     # 城市覆寫設定
```

### config/telegram.yaml（自行建立，不進 git）

```yaml
bot_token: "YOUR_BOT_TOKEN_HERE"
chat_id: "YOUR_CHAT_ID_HERE"
enabled: true
```

### Ready 門檻

| 條件 | 值 |
|------|-----|
| 最少 error_table 筆數 | 730 筆（約 2 年，覆蓋完整夏冬循環） |
| forecast recency | 最新 error_table date 距今 ≤ 7 天 |

---

## Fee 定案（2026-04-08）

| 參數 | 值 | 來源 |
|------|-----|------|
| fee_rate | 0.025 | 官方費率表 Weather 行 |
| fee_exponent | 0.5 | 官方費率表 Weather 行 |
| fee_maker | 0 | Maker 不收 fee |
| fee_maker_rebate | 0.25 | Taker fee 的 25% |

公式：`fee = C × p × feeRate × (p × (1-p))^exponent`

---

## Bug 修正紀錄（v6.3.0，共 20 項）

**第一批（Bug 1–10）**

| # | 檔案 | 問題 | 修正 |
|---|------|------|------|
| 1 | `signal_main.py` | subprocess timeout 硬編碼 | 改用 `timeout=600` 常數 |
| 2 | `15_alert_engine.py` | Telegram 訊息未截斷可能超限 | 訊息截斷 ≤ 4096 字元 |
| 3 | `telegram_bot.py` | 刪除 admin 後 in-memory 仍保留 | 立即 reload users.json |
| 4 | `collector_main.py` | backfill 結果未 reload csm | backfill 後立即 reload |
| 5 | `09_model_engine.py` | 模型訓練集未過濾 | 加日期過濾 |
| 6 | `11_ev_engine.py` | edge 計算邊界條件 | 修正 p=0/1 邊界 |
| 7 | `12_city_scanner.py` | 掃描結果未存 metadata | 補存 metadata 欄位 |
| 8 | `06_B_truth_fetch.py` | append 模式重複寫入 | 加去重檢查 |
| 9 | `08_market_price_fetch.py` | 報價快取過期未清 | 加 TTL 清除邏輯 |
| 10 | `02_init.py` | 目錄建立順序問題 | 修正建立順序 |

**第二批（Bug 11–20）**

| # | 檔案 | 問題 | 修正 |
|---|------|------|------|
| 11 | `07_daily_high_pipeline.py` | date filter 未傳入 run_step_a/b | 加 `start_date`/`end_date` 參數與過濾邏輯 |
| 12 | `07_daily_high_pipeline.py` | `int()` 截斷小數小時 | 改為 `round()` |
| 13 | `10_event_probability.py` | `lead_day` 缺失時無跳過保護 | 加 early `continue` + `log.warning` |
| 14 | `12_city_scanner.py` | 城市名大小寫不一致導致重複 | `normalize_city()` 加 `.title()` |
| 15 | `08c_book_state.py` | `last_event_str` 可能為 None | 加 `or ""` fallback |
| 16 | `16_position_manager.py` | `assert` 在 `-O` 模式下失效 | 全部改為 `raise RuntimeError` |
| 17 | `05_D_forecast_fetch.py` | horizon 超限未提前跳過外層迴圈 | 加外層 pre-check，跳過磁碟 I/O |
| 18 | `03_market_catalog.py` | `except Exception: pass` 靜默吞錯 | 改為 `log.warning` |
| 19 | `gen_dashboard.py` | 裸 `except:` 吞所有例外 | 改為 `except Exception as e: log.debug` |
| 20 | `01_main.py` | live mode 無 ready 城市時 fail-open | 改為 `sys.exit(1)` fail-closed |

---

## 各程式詳細說明

### `01_main.py` — 總入口 dispatcher

**職責**：按順序呼叫 02~11，管理執行狀態。

**CLI 參數**：

| 參數 | 說明 | 預設值 |
|------|------|--------|
| `--cities` | 城市（逗號分隔） | `London,Paris` |
| `--start-date` | 傳給 05/06/07 的起始日 | `""` |
| `--end-date` | 傳給 05/06/07 的結束日 | `""` |
| `--mode` | `live` 或 `historical` | `live` |
| `--skip-ev` | 跳過 11 | false |

**live mode 流程**：`02_init → 03 → 04 → 05 → 06 → 07 → 09 → 10 → 08 → 11`

**city_status.json 整合**：
- live mode 只處理 `status=ready` 的城市
- historical mode：`--cities` 不受限制（用於回補任意城市）

---

### `02_init.py` — 目錄結構初始化

建立所有必要目錄，檢查 Python 版本（>=3.9）和必要套件。

---

### `03_market_catalog.py` — 市場清單解析

從 Polymarket Gamma API 掃描所有天氣市場，用 regex 解析合約條件。

**輸出**：`data/03_market_catalog.csv`

---

### `04_market_master.py` — 市場主表

補充站點座標、城市名、metric_type 等 metadata。

**輸出**：`data/market_master.csv`

---

### `05_D_forecast_fetch.py` — GFS 預報抓取

從 Open-Meteo 抓取 GFS 逐小時預報。

**模式**：`--mode live`（最近 16 天）/ `--mode historical`（指定日期範圍）

**輸出**：`data/raw/D/{city}/gfs_seamless/forecast_hourly_{date}.csv`

---

### `06_B_truth_fetch.py` — 歷史真值抓取

從 Weather Underground 抓取各城市觀測站歷史最高溫。

**輸出**：`data/raw/B/{city}/truth_daily_high.csv`（append 模式）

---

### `07_daily_high_pipeline.py` — 誤差計算管線

- Step A：逐時預報 → 每日最高溫快照
- Step B：真值去重規範
- Step C：誤差表（forecast vs truth）

**輸出**：`data/processed/error_table/{city}/market_day_error_table.csv`

---

### `08_market_price_fetch.py` — 市場報價抓取

從 Polymarket CLOB API 抓取 best ask，同時透過 orderbook 計算深度（sweet spot / fixed depth）。

**輸出**：
- `data/raw/prices/market_prices.csv`
- `data/raw/prices/book_state/{market_id}.json`

---

### `09_model_engine.py` — ECDF 模型建立

從誤差表建立 empirical CDF，用於後續 p(YES) 計算。

**輸出**：`data/models/empirical/{city}/empirical_model.json`

---

### `10_event_probability.py` — 事件機率計算

用 ECDF 模型計算每個合約的 p(YES) 和 p(NO)。

**輸出**：`data/results/probability/{city}/event_probability.csv`

---

### `11_ev_engine.py` — EV 信號引擎

整合模型機率 + 市場報價，計算 edge、EV、Kelly，輸出交易信號。

**輸出**：`data/results/ev_signals/{city}/ev_signals.csv`

---

### `12_city_scanner.py` — 城市掃描器

掃描 Polymarket 有哪些新城市，自動建立 discovered 狀態。

---

### `13_city_status_manager.py` — 城市狀態管理

管理 `data/city_status.json`：discovered → backfilling → ready / failed。

**Ready 門檻**：
- error_table 筆數 ≥ 730
- 最新 error_table date 距今 ≤ 7 天
- empirical_model.json 存在且可讀

---

### `14_backfill_manager.py` — 歷史回補管理器

對 discovered 城市**逐城市**跑完整回補流程：05 → 06 → 07 → 09 → 10 → check ready。

- 一個城市失敗不阻塞其他城市
- 每完成一個城市立即更新 city_status.json（不等所有城市完成）
- 10（event_probability）跑完後才檢查 ready 條件，確保城市升 ready 後 signal 立刻可用
- failed 城市由 collector_main 在每次城市掃描成功後自動重置為 discovered（auto-retry）

**CLI**：
```bash
python 14_backfill_manager.py                          # 回補所有 discovered
python 14_backfill_manager.py --cities "Tokyo,Seoul"   # 只回補指定
python 14_backfill_manager.py --retry-failed            # 重跑 failed
python 14_backfill_manager.py --start-date 2023-01-01  # 指定起始日（預設）
```

---

### `15_alert_engine.py` — 通報引擎

篩選符合條件的信號，推送 Telegram 通報。

**通報門檻**（config/trading_params.yaml）：
- `alert_min_edge: 0.30`
- `alert_min_depth_usd: 100`
- `alert_cooldown_minutes: 10`

---

### `16_position_manager.py` — 持倉管理

記錄進場 / 平倉，管理 `data/positions.json`。UI 未開放，程式碼已實作。

---

### `collector_main.py` — Collector 長駐進程

排程執行 forecast/truth 更新、模型 rebuild、城市掃描、backfill。

---

### `signal_main.py` — Signal 長駐進程

排程執行報價更新（08）→ EV 計算（11）→ 通報（15）。每 5 分鐘一輪。

---

### `telegram_bot.py` — Telegram Bot

Bot UI 主程式。詳見上方「Telegram Bot UI」章節。

---

## data/ 目錄結構

```
data/
├── city_status.json           # 城市狀態（13 維護）
├── market_master.csv          # 市場主表（04 輸出）
├── positions.json             # 持倉記錄（16 維護）
├── _signal_state.json         # signal_main 狀態
├── _system_health.json        # 各進程心跳
├── raw/
│   ├── D/                     # GFS 預報原始資料
│   ├── B/                     # WU 真值原始資料
│   └── prices/
│       ├── market_prices.csv  # 最新報價快照
│       └── book_state/        # 各市場 orderbook
├── processed/
│   └── error_table/           # 誤差表（07 輸出）
├── results/
│   ├── ev_signals/            # EV 信號（11 輸出）
│   └── probability/           # 事件機率（10 輸出）
└── models/
    └── empirical/             # ECDF 模型（09 輸出）
```
