# Polymarket Weather Signal — 下階段開發規劃（主控文件）

> **文件性質**：活文件（Living Document）。每完成一個 STEP，更新狀態，不刪除舊內容。
> **用途**：開發藍圖 + 進度追蹤 + 決策主控。重置對話時貼這份 + README 就夠了。
> **版本**：v6（2026-04-09）
> **前置**：v4.2.0 pipeline 已完成（STEP 1–4 + 3.5 全部通過）
> **優先序**：若 README 與本文件衝突，以較新的決策紀錄為準；修正後兩份都要同步更新。

---

## 0. 文件維護規則

### 0.1 更新原則
1. 每完成一個 STEP → 更新狀態（⬜→🚧→✅）、驗收結果、決策紀錄、變更日誌
2. 不刪除舊內容，只做增補與修正
3. 設計被推翻 → 新增【修正紀錄：Before → After（原因）】

### 0.2 AI 使用規則
每次完成開發後，Claude Code / Codex 必須：
1. 先讀本文件 + README
2. 根據實作回寫本文件
3. 更新 README 受影響段落
4. 變更日誌新增一條
5. 偏離原規劃須寫進決策紀錄

---

## 1. 已完成（Phase 1）

| STEP | 內容 | 狀態 |
|------|------|------|
| STEP 1 | 清場，舊主線移到 legacy | ✅ |
| STEP 2 | 03/04 market-centered | ✅ |
| STEP 3 | forecast + truth + pipeline | ✅ |
| STEP 3.5 | previous_day + live + historical mode | ✅ |
| STEP 4 | ECDF → p_t → EV → signal | ✅ |
| 修補 | 05 probe / 06 append+skip / 08 價格 / 11 自動讀價 | ✅ |

**產出**：10,075 筆歷史誤差、London+Paris live EV、Technical+Signal PASS

---

## 2. Phase 2 主目標

把 MVP 升級成：可長駐、可擴城、可驗證、價格與信號層獨立運作。

### 非目標（鎖定不做）
- 自動下單
- 自動同步 Polymarket 持倉
- 多 metric 正式實作（只預留 interface）
- 複雜前端 / 手機 App

---

## 3. 架構原則

### 3.1 Collector / Signal 分離

```
Collector Layer（分鐘/小時/天級）
├── 城市掃描、歷史回補、forecast/truth 更新
└── 產出：模型、機率、城市狀態

Signal Layer（秒/事件級）
├── 只處理 ready 城市
├── 即時價格 → 即時 EV → 通報
└── 產出：price-aware EV、signal、alert
```

**核心原則：新城市 backfill 不能堵住已 ready 城市的 live signal。**

### 3.2 Signal Layer 共享輸出契約

Signal Layer 只讀以下檔案，不直接碰 raw fetch / backfill：

| 可讀（完整路徑） | 來源 |
|------|------|
| `data/city_status.json` | 13_city_status_manager |
| `data/market_master.csv` | 04_market_master |
| `data/results/probability/{city}/event_probability.csv` | 10_event_probability |
| `data/raw/prices/book_state/{market_id}.json` | 08_market_price_fetch |
| `data/raw/prices/market_prices.csv` | 08_market_price_fetch（fallback） |
| `data/positions.json` | 手動或 Bot |
| `data/models/empirical/{city}/empirical_model.json` | 09_model_engine |

不可直接碰：`data/raw/D/`、`data/raw/B/`、`data/processed/`、12/13/14 內部邏輯。

### 3.3 主專案保持輕便
只放 .py + config + 空目錄。不放 data。壓縮 < 5MB。

### 3.4 議題範圍
- 只做 daily_high（°C / °F）
- `metric_type` 全程傳遞但不實作多 metric 分支

---

## 4. 鎖定決策

| # | 決策 | 原因 |
|---|------|------|
| 1 | Fee 已定案（0.025 + exponent 0.5，見第 5 節） | 官方費率表數學驗證 |
| 2 | 先 REST snapshot 再 WebSocket | 先穩再快 |
| 3 | Bot 只做 UI，不做運算 | 職責分離 |
| 4 | 持倉先手動，不接自動同步 | 避免交易 API 複雜度 |
| 5 | metric_type 先傳遞不實作 | 預留擴充點 |
| 6 | stale/fee_unknown/book_incomplete → 不輸出正式 BUY/SELL | 安全原則 |
| 7 | 01_main.py 保留為手動全流程入口 | 向後相容 |
| 8 | `signal_status` 與 `signal_action` 分離 | 避免狀態與決策混淆 |

---

## 5. Fee 定案（2026-04-08）

### 5.1 結論

**Weather 類 taker fee：`fee_rate=0.025, exponent=0.5`。已通過數學驗證。**

### 5.2 驗證過程

**來源**：https://docs.polymarket.com/trading/fees — Upcoming Fee Structure（2026-03-30 生效，現已生效）

**官方頁面 Weather 行**：

| Category | Fee Rate | Exponent | Maker Rebate | Peak Effective Rate |
|----------|----------|----------|-------------|-------------------|
| Weather | 0.025 | 0.5 | 25% | 1.25% |

**官方公式**：
```
fee = C × p × feeRate × (p × (1 - p))^exponent
```

**官方費率表數學驗證（Weather, 100 shares）**：

| Price | 表上 Fee | 公式計算（0.025, exp=0.5） | 一致？ |
|-------|---------|------------------------|--------|
| $0.10 | $0.08 | 100×0.10×0.025×(0.09)^0.5 = 0.075 → $0.08 | ✅ |
| $0.50 | $0.62 | 100×0.50×0.025×(0.25)^0.5 = 0.625 → $0.62 | ✅ |
| $0.90 | $0.67 | 100×0.90×0.025×(0.09)^0.5 = 0.675 → $0.67 | ✅ |

**API base_fee 說明**：
- `GET /fee-rate?token_id={id}` 回傳 `{"base_fee": 1000}`
- 這個 1000 bps 是**下單簽名用的 feeRateBps**，不是 EV 計算用的 feeRate
- 我們目前不下單，不需要用 base_fee
- 未來下單時，簽名需帶 `feeRateBps: 1000`

### 5.3 爭議歷史

| 日期 | 事件 |
|------|------|
| 2026-04-07 | GPT 審核建議 fee_rate 應改為 0.05 |
| 2026-04-08 | Claude 查官方頁面，Weather 行顯示 0.025 + exponent 0.5 |
| 2026-04-08 | 數學驗證：0.025 版吻合官方費率表所有數字，0.05 版全部不吻合 |
| 2026-04-08 | API 查詢 base_fee=1000，確認為簽名用 bps，非計算用 feeRate |
| 2026-04-08 | **定案：fee_rate=0.025, exponent=0.5** |

**【修正紀錄】**
- Before：GPT 建議 Weather fee_rate = 0.05，主控文件一度標為「待定案」
- After：經官方費率表數學驗證 + API 查詢，定案為 0.025 + 0.5
- 原因：GPT 可能讀到不同版本頁面或混淆類別。數學驗算結論明確。

### 5.4 Maker fee

Maker 不收 fee（官方明確：maker fee rate = 0）。Maker rebate = 25%（taker fee 的 25% 分給 maker）。

---

## 6. STEP 路線圖

| STEP | 名稱 | 優先級 | 依賴 | 狀態 |
|------|------|--------|------|------|
| 5 | Fee 定案 + EV schema 升級 | 最高 | — | ✅ |
| 6 | 08 並行化 + 完整 orderbook | 高 | STEP 5 | ✅ |
| 7 | 城市掃描 + 狀態管理 | 高 | 可與 6 並行 | ✅ |
| 8 | Collector / Signal 正式分離 | 高 | STEP 7 | ✅ |
| 9 | 深度分析三模式 | 中 | STEP 6 | ✅ |
| 10 | 通報系統 | 中 | STEP 8+9 | ✅ |
| 11 | Telegram Bot | 中 | STEP 8+10 | ✅ |
| 12 | WebSocket 即時價格 | 低 | STEP 6 穩定 | ✅ |
| 13 | 持倉追蹤 + 出場邏輯 | 低 | STEP 10 | ✅ |
| 14A | 系統收尾（health + errors + fallback + smoke_test） | 低 | STEP 13 | ✅ |
| 14B | 部署阿里雲 | 低 | 14A | ⬜ |
| 15 | 議題擴充框架 | 低 | 任意 | ⬜ |

---

## 7. 各 STEP 詳細

### STEP 5：Fee 定案 + EV schema 升級 ✅

**Fee 已定案（見第 5 節），本步主要工作是 schema 升級和降級規則。**

要做的事：
1. `trading_params.yaml` 標記：
   ```yaml
   fee_mode: manual_hardcoded
   fee_basis: official_docs_2026_04_08
   fee_rate: 0.025
   fee_exponent: 0.5
   fee_maker: 0
   fee_maker_rebate: 0.25
   ```
2. `ev_signals.csv` 新增欄位：
   - `fee_mode`：`manual_hardcoded`
   - `fee_basis`：`official_docs_2026_04_08`
   - `fee_status`：`known`（Weather 已確認）/ `unknown`（非 Weather 類）
   - `price_status`：`fresh` / `stale`（price_age > 300s）
   - `book_source`：`rest_snapshot` / `ws_stream`
   - `price_age_seconds`
   - `signal_status`：`active` / `stale_price` / `fee_unknown` / `book_incomplete` / `token_mismatch` / `model_stale`
   - `signal_action`：`BUY_YES` / `BUY_NO` / `NO_TRADE` / `SUPPRESSED`
3. **`signal_status` 與 `signal_action` 正式分離**：
   - `signal_status` = 系統狀態（這筆信號的數據品質）
   - `signal_action` = 交易建議（BUY / NO_TRADE / SUPPRESSED）
   - 只有 `signal_status = active` 時才輸出正式 `signal_action`
4. 降級規則寫入程式：stale / unknown / incomplete → `signal_action = SUPPRESSED`
5. README + 本文件同步更新

驗收：
- [x] fee regression test：fee 計算與官方費率表 3 筆驗算一致（PASS: p=0.10/$0.08, p=0.50/$0.62, p=0.90/$0.67）
- [x] fee_mode / fee_basis 正確寫入（`manual_hardcoded` / `official_docs_2026_04_08`）
- [x] `fee_status = known` 時 EV 正常計算（Weather daily_high → known）
- [x] `signal_status = stale_price` 時 `signal_action = SUPPRESSED`（live 驗證通過）
- [x] 舊欄位保留（signal 欄位保留且值 = signal_action）
- [x] price_status / price_age_seconds 有值
- [x] live pipeline 跑通（London 11 rows, Paris 11 rows）

決策紀錄：
- 2026-04-08：fee 爭議經數學驗證定案。0.025 + 0.5 與官方費率表完全吻合。

---

### STEP 6：08 並行化 + 完整 orderbook ✅

要做的事：
1. 08 改用 ThreadPoolExecutor（10 線程），115s → ~15s
2. 新增完整 orderbook：`data/raw/prices/book_state/{market_id}.json`
   ```json
   {
     "market_id": "xxx",
     "market_slug": "...",
     "city": "London",
     "market_date_local": "2026-04-11",
     "contract_label": "Exactly 14°C",
     "metric_type": "daily_high",
     "yes_token_id": "yyy",
     "no_token_id": "zzz",
     "yes_bids": [{"price": "0.30", "size": "500"}, ...],
     "yes_asks": [{"price": "0.33", "size": "200"}, ...],
     "no_bids": [...],
     "no_asks": [...],
     "yes_best_bid": 0.30,
     "yes_best_ask": 0.33,
     "spread": 0.03,
     "book_timestamp_utc": "...",
     "snapshot_fetch_time_utc": "...",
     "source": "rest_snapshot",
     "is_stale": false
   }
   ```
3. `market_prices.csv` 保留（向後相容）
4. 11 改讀 book_state（優先）→ fallback 讀 market_prices.csv

驗收：
- [x] 08 < 20 秒（實測 18.9s，110 markets，舊版 ~115s）
- [x] book_state JSON 有完整 bids/asks + debug 欄位（city, contract_label, metric_type 等）
- [x] market_prices.csv 仍正常
- [x] 11 優先讀 book_state，price_age 用 snapshot_fetch_time_utc 精確計算
- [x] book_source = rest_snapshot / csv_fallback 正確標記
- [x] 429 重試邏輯實作（sleep 5s + 1 次重試）

決策紀錄：
- 2026-04-08：book_state JSON 加入 city/market_date_local/contract_label/metric_type 等 debug 友善欄位（GPT 建議）。

---

### STEP 7：城市掃描 + 狀態管理 ✅

新增檔案：
- `12_city_scanner.py`：Gamma API 掃描 → 解析城市 → 更新 city_status.json
- `13_city_status_manager.py`：城市狀態機（CityStatusManager class + CLI）
- `14_backfill_manager.py`：discovered 城市回補流程（05→06→07→09）

Part 0 前置修正：
- `07_daily_high_pipeline.py`：station metadata fallback 改為 per-station merge（market_master 優先，seed 填空缺）
- `08_market_price_fetch.py`：book_state JSON 新增 `book_complete`（bool）+ `fetch_duration_ms`（int）
- `11_ev_engine.py`：signal_status 優先序改為 no_price > book_incomplete > stale_price > fee_unknown > active；新增 `book_complete` 參數

城市狀態機：
```
discovered → backfilling → ready
     ↓           ↓           ↓
 no_metadata   failed     disabled
                 ↓
            backfilling（retry）
```

合法轉移：
```
discovered  → backfilling | disabled
backfilling → ready | failed
failed      → backfilling（retry）
ready       → disabled
no_metadata → discovered（metadata 補齊後）
```

city_status.json schema：status, city, timezone, station_code, country, supported_metrics,
discovered_at_utc, last_scan_at_utc, last_backfill_start_utc, last_backfill_end_utc,
last_ready_utc, earliest_forecast_date, latest_forecast_date, error_row_count,
market_count_active, failure_count, note, updated_at_utc

**ready 門檻（暫定 MVP 值，非正式充分條件）**：
- `error_row_count >= 100`
- `empirical_model.json` 存在且可讀

01_main.py 整合（city_status.json 存在時）：
- live mode：只跑 ready 城市；--cities 是 filter（非 ready 不能強制進）
- historical mode：--cities 不受限制（用於回補）
- 向後相容：city_status.json 不存在 → 完全退回舊行為

London/Paris 自動偵測：error_table >= 100 + model 存在 → 自動標 ready

驗收：
- [x] 13 city_status.json 格式正確（station_code 欄位、原子寫入）
- [x] 非法轉移被擋掉（ValueError + CLI 拒絕）
- [x] 12 能掃出 Polymarket 城市 / no_metadata / discovered
- [x] 既有城市 not 重置 status，只更新 market_count_active
- [x] no_metadata → discovered（seed 補齊後）
- [x] 14 discovered → backfilling → ready/failed（ready 條件 >= 100 + model）
- [x] 失敗 → failed（不阻塞其他城市）
- [x] --retry-failed 可重跑
- [x] 每完成一城市立即更新 city_status.json
- [x] London+Paris 自動偵測為 ready
- [x] live mode --cities 只過濾 ready 城市
- [x] historical mode --cities 可指定任意城市
- [x] city_status.json 不存在 → 退回舊行為
- [x] book_complete 欄位在 book_state JSON 中正確
- [x] fetch_duration_ms 在 book_state JSON 中正確
- [x] 07 per-station seed fallback（market_master 缺席時自動補）
- [x] signal_status = no_price（無價格時，與 book_incomplete 分開）

決策紀錄：
- 2026-04-08：station_code（seed）vs station_id（market_master）統一策略：city_status.json 用 `station_code`（與 seed 一致），07 per-station merge（market_master 優先，seed 補缺）。
- 2026-04-08：signal_status 新增 `no_price` 作為獨立狀態（vs `book_incomplete`）。no_price = 根本沒有價格，book_incomplete = 有價格但 bids/asks 不完整。
- 2026-04-08：ready 門檻 100 為暫定值，非正式充分條件。

---

### STEP 8：Collector / Signal 正式分離 ✅

新增：`collector_main.py`、`signal_main.py`

Part 0 前置修正：
- `13_city_status_manager.py`：新增 `bootstrap()` 方法（bootstrap ready 邏輯歸位）、`update_build_time()` 方法、city_status.json 新增欄位 `schema_version` / `last_error` / `probability_build_time_utc` / `model_build_time_utc`；`set_failed()` 同時寫入 `last_error`
- `08_market_price_fetch.py`：新增 `verify_book_csv_consistency()` 全量比對（非阻塞），在 `run()` 末尾呼叫
- `01_main.py`：移除私有 helper（`_auto_init_city_status`, `_count_error_rows`, `_model_exists`, `_load_seed_city`），改為呼叫 `csm.bootstrap()`
- `PHASE2_ROADMAP.md`：3.2 共享輸出契約補全完整 per-city 路徑

collector_main.py：
- `CollectorScheduler`：對齊時間點排程（避免漂移）
  - 掃描間隔 24h、forecast 更新 6h、truth 更新 24h
- 主循環：12 掃描 → 14 回補（discovered）→ 05+07+09+10（forecast）→ 06+07+09+10（truth）
- 每城市失敗不阻塞其他城市
- 更新後呼叫 `csm.update_build_time(city, "probability" / "model")`
- CLI：`--once`（測試）、`--verbose`

signal_main.py：
- `SignalLoopState`：運維健康狀態（last_success_utc, last_cycle_duration_ms, consecutive_failures, last_error, total_cycles, total_failures）
- 主循環：csm.get_ready_cities() → 08 → 11 → alert_hook
- 不重疊：`interval - elapsed` 控制，不是固定 sleep
- 退避：連續失敗 >= 3 → 最少 60 秒
- alert_hook：callback 參數預留（STEP 10 接上 15_alert_engine.py）
- CLI：`--once`、`--interval`（預設 30s）、`--verbose`

01_main.py 保留為手動全流程入口（向後相容）。

驗收：
- [x] collector --once 正常跑一次
- [x] 排程用時間點對齊（CollectorScheduler），不是固定 sleep
- [x] 掃描 / 回補 / forecast / truth 各自按節奏執行
- [x] 更新後重算 07→09→10 + 更新 build metadata
- [x] 一個城市失敗不阻塞其他（per-city try/except）
- [x] signal --once 正常跑一次
- [x] signal 只讀 ready 城市
- [x] 不重疊（interval - elapsed）
- [x] 退避（consecutive_failures >= 3 → 最少 60s）
- [x] loop state 有 last_success / consecutive_failures / last_error
- [x] alert hook 預留（callback 參數，非假檔案）
- [x] bootstrap() 從 13 統一管理（01_main 不自己判斷）
- [x] city_status.json 有 schema_version / last_error / probability_build_time_utc / model_build_time_utc
- [x] 共享輸出契約改成 per-city 路徑（3.2）
- [x] book_state vs CSV 全量一致性檢查（非阻塞 log）
- [x] 01_main.py 不受影響（向後相容）

決策紀錄：
- 2026-04-08：bootstrap ready 邏輯從 01_main.py 移到 13_city_status_manager.py，避免其他入口（collector_main, signal_main, bot）各自複製規則。
- 2026-04-08：SignalLoopState 只記運維健康，alert memory（cooldown map）等 STEP 10 才加，不預建空殼。
- 2026-04-08：alert_hook 設計為 callback 參數（而非假檔案），STEP 10 直接接上 15_alert_engine.py。

---

### STEP 9：深度分析三模式 ✅

新增：`_lib/fill_simulator.py`

Part 0 STEP 8 收尾補丁：
- `08/09/10/11`：最終輸出改為**原子寫入**（tempfile + os.replace），防止 signal 層讀到半寫狀態
- `signal_main.py`：補齊共享輸出契約文件（只讀 finalized outputs + ❌ 清單）；補充城市 model 失敗降級規則（跳過城市，不整輪失敗）
- `collector_main.py`：在 update_build_time 呼叫處加注釋「只在檔案落盤後才更新」

fill_simulator.py 設計：
- 純函式（不讀檔、不打 API、不碰 config）
- `FillLevel` / `FillResult` dataclasses
- `simulate_fill(orderbook_levels, p, fee_rate, fee_exponent, mode, fixed_depth_usd, side)`
- 三種 mode 共用一個函式，termination 用明確參數
- `fixed_depth`：最後層支援 partial fill；`depth_exhausted=True` 表示 book 不夠深
- `sweet_spot`：第一個 marginal_ev ≤ 0 即停
- `test_fill_simulator()` regression test（可 `python _lib/fill_simulator.py` 直接跑）

11_ev_engine.py 整合：
- 從 `_lib/fill_simulator.py` 動態載入（sys.path 注入），import 失敗不阻塞管線
- `ev_signals.csv` 新增 22 個深度分析欄位（yes/no × sweet/fixed × shares/usd/avg_price/ev/exhausted + yes/no_depth_usd）
- 有 book_state + fill_simulator 時填充；否則空字串（不影響 signal_status）

config/trading_params.yaml 新增：
```yaml
depth_fixed_usd: 200    # fixed_depth 分析金額（不等於 STEP 10 alert_min_depth_usd）
depth_ema_alpha: 0.3    # EMA 預留（第一版不啟用）
depth_ema_enabled: false
```

驗收：
- [x] _lib/fill_simulator.py 存在
- [x] 純函式（不讀檔、不打 API）
- [x] 三種 mode 共用同一個函式
- [x] sweet_spot 遇第一個 marginal_ev ≤ 0 就停
- [x] fixed_depth 支援 partial fill（最後層只吃部分）
- [x] depth_exhausted 正確標記（fixed/sweet）
- [x] regression test：best_only / sweet_spot / fixed_depth 各通過
- [x] best_only 結果 = 舊 11 edge/EV 向後相容
- [x] empty book 回傳空 FillResult，不 crash
- [x] ev_signals.csv 有 sweet_spot / fixed_depth 新欄位
- [x] 舊欄位（edge / EV）不受影響
- [x] 沒有 book_state 或 fill_simulator 時，深度欄位為空字串（不 crash）
- [x] 08/09/10/11 原子寫入
- [x] signal_main.py 共享契約文件補齊

決策紀錄：
- 2026-04-08：depth_fixed_usd=200 是分析預設值，不等於 STEP 10 的 alert_min_depth_usd（兩者獨立）。
- 2026-04-08：fill_simulator 用動態載入（sys.path），import 失敗僅 log warning，深度欄位為空，不阻塞管線。
- 2026-04-08：EMA 功能預留（depth_ema_alpha/depth_ema_enabled 進 config），第一版不啟用。

---

### STEP 10：通報系統（AlertEngine） ✅

新增：`15_alert_engine.py`

**設計原則**：
- 第一版只做進場通報，出場 hook 預留給 STEP 13
- 寧可漏推不可錯推：所有非 active 狀態都 suppress（包括 model_stale）
- AlertEngine 吃 in-memory 結果，不重讀 CSV
- Telegram 是可選的：沒有 config 就只寫 log

**11_ev_engine.py 修改**：
- `run()` 回傳型別從 `bool` 改為 `tuple[bool, list[dict]]`
- 收集所有城市的 output_rows 成 `all_rows`（in-memory）
- `__main__` block 改為 `ok, _ = run(...)`

**signal_main.py 修改**：
- 新增 `_get_ev_engine_mod()` / `_get_alert_engine_mod()` lazy-load helpers（module 快取）
- `_run_ev_engine(cities, verbose)` → in-process 呼叫 11，回傳 `(bool, list[dict])`
- `_load_params()` → 讀 trading_params.yaml 為 flat dict
- `_setup_alert_engine()` → 啟動時初始化 AlertEngine + load_cooldown_from_history
- cycle 內：`ok_ev, ev_results = _run_ev_engine(ready)` → `alert_engine.evaluate(ev_results)` → `alert_engine.process(alerts)`
- 保留 `alert_hook` 參數（backward compat）

**config/telegram.yaml（新，不進 repo）**：
- 路徑：`config/telegram.yaml`
- 欄位：`bot_token`, `chat_id`, `enabled`
- 環境變數優先：`PM_TELEGRAM_BOT_TOKEN`, `PM_TELEGRAM_CHAT_ID`

**進場條件（全部同時滿足）**：

| 條件 | config 鍵 | 預設值 | 說明 |
|------|-----------|--------|------|
| signal_status == "active" | — | — | 非 active 一律 suppress |
| signal_action in BUY_YES/BUY_NO | — | — | NO_TRADE / SUPPRESSED 不推 |
| edge >= min_edge | alert_min_edge | 0.30 | 方向對應（BUY_YES→yes_edge） |
| sweet_spot_usd >= min_depth_usd | alert_min_depth_usd | 100 | 有深度資料時才檢查 |
| sweet_spot_ev > 0 | alert_require_positive_ev | true | 有深度資料時才檢查 |
| settlement >= min_settlement_hours | alert_min_settlement_hours | 2 | 距結算小時數 |
| 不在 cooldown 內 | alert_cooldown_minutes | 10 | key=(market_id, signal_action) |

**Cooldown 設計**：
- key = `(market_id, signal_action)`（不只用 market_id，允許方向翻轉）
- 跨重啟恢復：`load_cooldown_from_history()` 讀最近 1 天的 alert_history CSV
- 吃 cooldown 先於 Telegram 推送（不管送達與否）

**Alert History（日切檔）**：
- 路徑：`logs/15_alert/YYYY-MM-DD_alert_history.csv`
- 欄位：generated_utc, market_id, city, market_date, contract, signal_action, edge, ev, sweet_spot_usd, sweet_spot_avg_price, depth_basis, edge_basis, settlement_hours, sent_telegram, send_error, cooldown_applied

**Stale 抑制表（第一版全部 suppress）**：

| signal_status | 第一版處理 |
|---------------|----------|
| active | 正常評估 |
| stale_price | suppress |
| fee_unknown | suppress |
| book_incomplete | suppress |
| token_mismatch | suppress |
| no_price | suppress |
| model_stale | suppress（第一版不推 warning） |

**check_exits()** 預留為 `pass`（STEP 13 實作）。

驗收：
- [x] AlertEngine 可獨立實例化和測試
- [x] evaluate() 吃 in-memory dict list，不讀 CSV
- [x] 全部條件滿足 → 生成 alert
- [x] 任一條件不滿足 → 不生成
- [x] cooldown key = (market_id, signal_action)
- [x] cooldown 跨重啟恢復（從 history 重建）
- [x] 城市級限流（max_per_city_per_cycle）
- [x] 全局限流（max_total_per_cycle）
- [x] model_stale → suppress（第一版）
- [x] check_exits() 留空 hook
- [x] TelegramSender timeout=10s，失敗回傳 (False, error_msg)，不 raise
- [x] 推送失敗仍吃 cooldown
- [x] send_error 記錄到 alert_history
- [x] alert_history 日切檔（YYYY-MM-DD_alert_history.csv）
- [x] cooldown 永遠 applied（不管 send 成功與否）
- [x] trading_params.yaml 有完整 alert 區段
- [x] telegram.yaml 不進 repo（.gitignore）
- [x] Telegram 載入：環境變數優先 → yaml 次之
- [x] 沒有 Telegram config → 只寫 log 不報錯
- [x] signal_main.py 的 alert_hook 向後相容保留
- [x] ev_results 是 in-memory 傳入（不重讀 CSV）
- [x] 沒有 Telegram → alert 仍正常 evaluate + 寫 history
- [x] PHASE2_ROADMAP.md STEP 10 更新為 ✅
- [x] README.md 同步更新

**決策紀錄**：
- 2026-04-08：11_ev_engine.run() 改為回傳 (bool, list[dict])，__main__ 用 `ok, _ = run(...)`，subprocess 呼叫不受影響（exit code 機制不變）。
- 2026-04-08：signal_main 不再用 subprocess 跑 11，改為 in-process importlib，好處是零 IPC 開銷、直接拿 in-memory 結果，不需重讀 CSV。
- 2026-04-08：_ev_engine_mod / _alert_engine_mod 用模組快取（只 exec_module 一次），避免每輪重新載入的開銷和 fill_simulator 重複 import。
- 2026-04-08：depth 欄位條件（has_depth check）：ev_signals 來自 in-memory，若 fill_simulator 不可用或 book_state 缺失，yes_sweet_usd 欄位不存在於 dict，has_depth = False → depth/ev 條件自動跳過，不阻擋進場評估。

---

### STEP 11：Telegram Bot ✅

**Bot 是 UI，不是運算主體。只讀數據。分進程運行。**

**新增檔案**：
- `telegram_bot.py` — Bot 主程式（WeatherSignalBot + main()）
- `_lib/signal_reader.py` — Bot 資料讀取層（SignalDataReader，只讀 finalized outputs）

**STEP 10 小補丁（已完成）**：
- 0.1：`has_depth=False` → evaluate() 直接 suppress，不推送 entry alert
- 0.2：alert_history 新增 `alert_key` 欄位（`{market_id}|{signal_action}`）
- 0.3：telegram.yaml 新增 `allowed_chat_ids` / `admin_chat_ids`

**signal_main.py 小改**：
- `_check_refresh_requested()` — 檢查 `data/_refresh_requested` flag
- `_sleep_with_refresh_check(total_sleep)` — 每 5 秒輪詢 refresh flag，可提前結束 sleep
- `_write_signal_state(loop_state, ready_count)` — 原子寫入 `data/_signal_state.json`

**Bot 功能一覽**：

| 功能 | callback | 說明 |
|------|----------|------|
| /start / /menu | — | 主選單（顯示 ready 城市數 + 最近刷新時間） |
| 刷新資料 | refresh | admin only，寫 data/_refresh_requested |
| 選城市 | cities | 列出 ready 城市 + 最新日期 |
| 城市信號 | city:{city}:{date} | 該城市該日所有合約，含翻日期 ◀️▶️ |
| 信號排行 | rank:{sort}:{offset} | 跨城市 active 信號，8 筆/頁，排序切換 |
| 信號詳情 | detail:{market_id_prefix} | 完整欄位（含深度） |
| 通報歷史 | history | 最近 24h alert_history，最多 20 筆 |
| 設定 | settings | 顯示 trading_params.yaml 關鍵欄位（只讀） |
| 城市管理 | city_mgmt | 分類顯示 city_status.json |
| 觸發掃描 | scan_cities | admin only，寫 data/_scan_requested |

**Signal Reader 設計**：
- `get_ready_cities()` → city_status.json（status=ready）
- `get_city_signals(city, date)` → ev_signals/{city}/ev_signals.csv
- `get_available_dates(city)` → unique market_date_local
- `get_all_signals_ranked(sort_by, limit, offset)` → 跨城市 active，paginated
- `get_signal_detail(market_id_prefix)` → 前綴匹配
- `get_alert_history(hours)` → logs/15_alert/日切 CSV
- `get_trading_params()` → config/trading_params.yaml
- `get_signal_state()` → data/_signal_state.json
- `request_refresh()` → touch data/_refresh_requested
- `request_city_scan()` → touch data/_scan_requested

**依賴**：
```bash
pip install python-telegram-bot  # v20+，async 版本
```

**啟動**：
```bash
python telegram_bot.py           # 分進程啟動（與 signal_main.py 無關）
```

驗收：
- [x] /start 顯示主選單（含 ready 城市數 + 最近刷新時間）
- [x] 非 allowed_chat_id → 拒絕（⛔ 無權限）
- [x] 選城市 → 顯示信號（正負 edge 都有）
- [x] 翻日期正常（◀️ / ▶️）
- [x] 信號排行 + 翻頁（每頁 8 筆）
- [x] 排序切換（edge / ev / depth）
- [x] 信號詳情頁（含深度）
- [x] 通報歷史（24 小時）
- [x] 設定頁只讀
- [x] 城市管理頁（分類顯示，admin 可觸發掃描）
- [x] 刷新按鈕 → admin only → 寫 refresh flag
- [x] 掃描按鈕 → admin only → 寫 scan flag
- [x] Bot handler 不做任何 API call 或重算（只讀 SignalDataReader）
- [x] signal_main 寫 _signal_state.json（Bot 讀取最近刷新時間）
- [x] signal_main 支援 _refresh_requested flag（每 5s 輪詢，可提前結束 sleep）
- [x] has_depth=False → suppress（STEP 10 補丁）
- [x] alert_key 欄位（STEP 10 補丁）
- [x] telegram.yaml allowed/admin chat ids（STEP 10 補丁）
- [x] PHASE2_ROADMAP.md STEP 11 更新為 ✅
- [x] README.md 同步更新

**決策紀錄**：
- 2026-04-08：Bot 不暴露「觸發回補」按鈕，回補由 collector_main 自動偵測 discovered 城市執行，Bot 只觸發城市掃描。
- 2026-04-08：callback_data market_id 使用前 46 字元作前綴匹配，在 64 bytes Telegram 限制內，實際唯一性足夠。
- 2026-04-08：_sleep_with_refresh_check 以 5 秒為輪詢間隔，在響應性和 CPU 使用之間取平衡。signal state JSON 原子寫入，Bot 讀取不會看到 partial write。
- 2026-04-08：has_depth=False suppress 是 STEP 10 的補丁，比原設計更保守（原本只跳過深度條件，現在直接不推送），對應原則「寧可漏推不可錯推」。

---

### STEP 12：WebSocket 即時價格 ✅

**12A（ingestion）✅；12B-1（signal_main --mode ws）✅；12B-2（自動 fallback / 預設切換）延後至 STEP 14 前執行。**

endpoint：`wss://ws-subscriptions-clob.polymarket.com/ws/market`
新增：`08b_price_stream.py`、`08c_book_state.py`
補丁：STEP 11 小補丁（refresh flag JSON + signal_main last_refresh_completed_utc）

**架構**：

```
08b_price_stream.py（asyncio 常駐）
  └─ PriceStreamListener
       ├─ rest_bootstrap()    用 08 的 fetch_book_with_retry 建初始 snapshot
       ├─ connect/subscribe   wss://ws-subscriptions-clob.polymarket.com/ws/market
       ├─ _handle_book        WS book event → apply_side_snapshot（單 token）
       ├─ _handle_price_change WS price_change → apply_price_change（O(1)）
       │     size="0" → 移除 price level
       ├─ _handle_last_trade_price → last_trade_price（不影響 orderbook）
       ├─ flush_loop          每 5 秒 flush dirty books（原子寫入）
       ├─ verify_consistency  每 5 分鐘抽 3 market 比對 WS vs REST
       └─ reconnect           指數退避（5→10→20→...→120s），重連後 REST bootstrap

08c_book_state.py（data structure only）
  ├─ OrderBook              dict-based（O(1) 查找），dirty flag，stale 檢測
  │    ├─ apply_snapshot       REST 全量 bootstrap（四 side 一起替換）
  │    ├─ apply_side_snapshot  WS book event（單 token，不影響另一 side）
  │    ├─ apply_price_change   WS 增量（size=0 刪除，size>0 新增/更新）
  │    └─ to_book_state_dict   輸出與 08 REST 版完全相容 + WS 專屬欄位
  └─ BookStateManager       get_or_create, flush_dirty（debounce 1s）, flush_all, mark_all_stale
```

**STEP 11 補丁（12A 前置）**：
- refresh flag 從空檔 → JSON（`requested_at_utc` + `requested_by_chat_id`）
- signal_reader.request_refresh(chat_id) 寫 JSON
- telegram_bot.cb_refresh 傳 chat_id
- signal_main._check_refresh_requested() 讀 JSON 回傳 metadata
- SignalLoopState 新增 `last_refresh_completed_utc` + `_pending_refresh_by`
- _signal_state.json 新增 `last_refresh_completed_utc` 欄位

**WS 訊息格式**：
```json
// subscribe
{"assets_ids": ["token_id_1", ...], "type": "subscribe"}

// book event（全量 snapshot，per token）
{"event_type": "book", "asset_id": "token_id", "bids": [...], "asks": [...]}

// price_change（增量，size="0" 表示移除）
{"event_type": "price_change", "asset_id": "token_id",
 "changes": [{"price": "0.50", "size": "0", "side": "BID"}]}

// last_trade_price（附加資訊，不影響 orderbook）
{"event_type": "last_trade_price", "asset_id": "token_id", "price": "0.505"}
```

**12B-1（完成）**：signal_main 新增 `--mode ws`，背景啟動 08b，每 5 秒用 in-memory book 跑 11 + 15。

**12B-2（完成，於 STEP 14A 執行）**：自動 fallback（WS 掉線 → REST 降級）+ 預設模式切換（不帶 --mode → 嘗試 WS，失敗退 REST）。

**12A 補丁（12B-1 前置）**：
- price_change.side 映射：BUY→bids / SELL→asks（官方 WS 推 BUY/SELL，不是 BID/ASK）
- best_bid_ask 事件：log.debug + 忽略（自己從 dict 算 best）
- flush 規則文件化（08c_book_state.py docstring）
- 08c 新增 get_book() 方法
- 08b 新增 _bootstrap_done asyncio.Event（signal_main 等 bootstrap 完成用）

**12B-1 架構**：

```
signal_main --mode ws
  ├─ load_markets（08b.load_markets，含 token_id）
  ├─ BookStateManager + PriceStreamListener（08c + 08b）
  ├─ ws_task = asyncio.create_task(stream.run())   # 背景
  ├─ await _wait_for_bootstrap(stream, 60s)        # 等 REST bootstrap
  └─ 主循環（每 5 秒）
       ├─ bsm.get_book → book.to_book_state_dict() → books_in_memory
       ├─ asyncio.to_thread(_run_ev_engine, book_source="memory")
       ├─ alert_engine.evaluate + process
       ├─ bsm.flush_dirty（供 Bot 讀）
       └─ _write_signal_state（price_mode="ws", ws_connected, last_ws_event_utc）
```

驗收：
#### STEP 11 補丁
- [x] refresh flag 改成 JSON（含 timestamp + chat_id）
- [x] signal_main 消費後寫 last_refresh_completed_utc
- [x] telegram_bot cb_refresh 傳 chat_id

#### 08c_book_state.py
- [x] OrderBook 內部用 dict（O(1) 查找）
- [x] apply_snapshot 正確載入完整 book（REST 全量）
- [x] apply_side_snapshot 更新單 token side，不影響另一 side（WS book event）
- [x] apply_price_change：size>0 更新，size=0 刪除
- [x] is_stale() per-market 判斷（300s 門檻）
- [x] dirty + flush_dirty 只寫有變更的 book（debounce 1s）
- [x] flush 輸出的 JSON 與 STEP 6 book_state schema 完全相容
- [x] last_event_utc / event_count_since_snapshot 正確追蹤

#### 08b_price_stream.py
- [x] 能連線到 Polymarket WebSocket
- [x] 訂閱用 asset_id（token_id）
- [x] 收到 book event → apply_side_snapshot（per token）
- [x] 收到 price_change → apply_price_change
- [x] size="0" → 正確移除 price level
- [x] last_trade_price → 更新但不影響 orderbook
- [x] 支援 list of events（初始推送格式）
- [x] 斷線 → 指數退避重連（5→10→20→...→120s）
- [x] 重連後 REST bootstrap 重建 state
- [x] REST bootstrap 用 asyncio.run_in_executor 包同步呼叫

#### 一致性驗證
- [x] 啟動時比對（WS 訂閱後等 2s 讓 book events 進來）
- [x] 每 5 分鐘抽查 3 個 market（WS state vs REST snapshot）

#### 獨立運行
- [x] `python 08b_price_stream.py --cities "London,Paris"` 可獨立啟動
- [x] `--once`：REST bootstrap 快照一次後退出（測試用）
- [x] 每 5 秒 flush book_state JSON（保底）
- [x] 輸出的 JSON 可被現有 11 / Bot 正常讀取
- [x] Ctrl+C 優雅退出（最後 flush 一次）

#### 12A 補丁
- [x] price_change.side BUY/SELL → bids/asks 映射（SIDE_MAP 常數）
- [x] best_bid_ask 事件 log.debug + 忽略
- [x] flush 規則文件化（BookStateManager docstring）
- [x] 08c 新增 get_book() 方法（不自動創建）
- [x] 08b 新增 _bootstrap_done asyncio.Event

#### 11 支援 in-memory book
- [x] run() 新增 book_source="json" / books_in_memory=None 參數
- [x] book_source="memory"：從 books_in_memory dict 讀，不讀磁碟
- [x] WS 模式下 memory 裡沒有 book → log warning + 該市場跳過
- [x] book_source="json" 行為不變（向後相容）

#### signal_main --mode ws
- [x] --mode ws 可啟動（asyncio.run(run_ws_mode)）
- [x] 08b 在背景 asyncio task 運行
- [x] 等 REST bootstrap 完成（_wait_for_bootstrap，最多 60 秒）才進主循環
- [x] 每 5 秒用 in-memory book 跑一輪 11 + 15
- [x] 不重疊：interval - elapsed 計算
- [x] 退避：consecutive_failures >= 3 → 最少等 60 秒
- [x] 11 用 asyncio.to_thread() 包裝（11 內部不改）
- [x] alert 正常工作（同 REST 模式）
- [x] flush dirty books（供 Bot 讀）
- [x] _signal_state.json 有 price_mode / ws_connected / last_ws_event_utc

#### 向後相容
- [x] --mode rest 行為完全不變
- [x] 不帶 --mode → 嘗試 WS，WS 啟動失敗自動退 REST（12B-2，於 14A 完成）
- [x] --once 在兩種模式下都正常
- [x] refresh flag 在 WS 模式下仍生效（async sleep）

#### 文件
- [x] PHASE2_ROADMAP.md STEP 12 更新為 ✅（12B-2 於 STEP 14A 完成）
- [x] README.md 同步更新（signal_main --mode ws 說明）

**決策紀錄**：
- 2026-04-09：12A 只做 ingestion，不改 signal_main。signal_main 繼續用 08 REST 版。12B 才接 WS。
- 2026-04-09：WS book event 是 per-token（只含一個 token 的 bids/asks），用 apply_side_snapshot 更新單 side，保留另一 side 的狀態。不用 apply_snapshot（那是 REST 全量用的）。
- 2026-04-09：REST bootstrap 用 asyncio.run_in_executor 包同步呼叫，避免 blocking event loop。
- 2026-04-09：market_master.csv 確認有 yes_token_id / no_token_id，12A 前提已滿足。
- 2026-04-09：12B-1 不改預設模式。--mode ws 手動指定。12B-2 才做自動 fallback 和預設切換。
- 2026-04-09：price_change.side 官方 WS 實際推的是 BUY/SELL，不是 BID/ASK。12A 初版寫錯，12B-1 前置補丁修正。SIDE_MAP = {"BUY": "bids", "SELL": "asks"}。
- 2026-04-09：12B-2（WS 自動 fallback + 預設切換）延後至 STEP 14 前。理由：在無穩定部署環境下設計 fallback 是過度設計；STEP 13 持倉邏輯優先度更高。
- 2026-04-09：STEP 13 第一版只做手動記錄，不接 Polymarket 交易 API。PnL 用持有 token 的 best_bid 計算（賣出方向）。EXIT 只在 signal_status == active 時觸發，非 active 發 warning 不發 EXIT。
- 2026-04-09：11_ev_engine.py 新增 yes_best_bid / no_best_bid 到 out_row，屬於輸出 schema 擴充，不改動核心 EV 計算邏輯。

---

### STEP 13：持倉追蹤 + 出場邏輯 ✅

**手動記錄進場（Bot 按鈕）。不接交易 API。持續監控 edge，翻負 → EXIT 通報。**

**新增檔案**：
- `16_position_manager.py`：`PositionManager` 類，讀寫 `data/positions.json`
  - `add_position()` / `close_position()` → 立即原子寫入（write-through）
  - `update_mark()` → 記憶體更新（含 unrealized_pnl_gross / net）
  - `flush_edges()` → 節流 30 秒落盤

**修改檔案**：
- `15_alert_engine.py`：
  - 實作 `check_exits()` → `(exit_alerts, warnings)`；只有 `signal_status == active` 且 edge < 0 才觸發
  - 新增 `process_exits()`：推 Telegram + 寫 `logs/15_exit/` 日切檔
  - EXIT cooldown 30 分鐘；edge 轉正再轉負自動重置（`_edge_crossed_positive` 追蹤）
- `11_ev_engine.py`：`out_row` 新增 `yes_best_bid` / `no_best_bid`（來自 book_state）
- `signal_main.py`：REST + WS 模式都初始化 `PositionManager`，每輪呼叫 `_update_positions()`
- `_lib/signal_reader.py`：新增 `get_open_positions()` / `get_closed_positions()` / `get_position()`
- `telegram_bot.py`：
  - 主選單新增「💼 我的持倉」
  - 信號詳情頁（admin only）新增「📝 記錄進場」
  - 進場 `ConversationHandler`：價格 → 股數 → 確認（3 步驟）
  - 平倉 `ConversationHandler`：價格 → 確認（2 步驟）
  - `cb_positions()` 頁：open（含 edge/PnL 即時）+ closed（最近 5 筆）

**驗收結果**：
- [x] `16_position_manager.py` 語法檢查 OK
- [x] positions.json 原子寫入
- [x] add_position / close_position 立即落盤
- [x] update_mark 記憶體更新，flush_edges 節流 30 秒
- [x] unrealized_pnl_gross / net 分開計算
- [x] check_exits() → signal_status active + edge < 0 → EXIT
- [x] 非 active → warning，不推 EXIT
- [x] EXIT cooldown 30 分鐘，edge 轉正重置
- [x] Bot 持倉頁、進場 / 平倉 ConversationHandler
- [x] REST + WS 模式都支援持倉追蹤
- [x] 所有 6 個修改/新增檔案語法檢查 OK

---

### STEP 14A：系統收尾 ✅

**讓 3 個進程能穩定運行、互相可觀測，為部署做好準備。**

**改動清單**：

| 檔案 | 改動 |
|------|------|
| `telegram_bot.py` | 新增 `UserManager`（users.json 獨立）；新增 `_update_system_health()`；bootstrap users.json from chat_id |
| `signal_main.py` | 新增 `_update_system_health()`；新增 `ErrorReporter`；新增 `_setup_shutdown_hooks()`；12B-2 WS→REST fallback；`_signal_state.json` 新增 ws_fallback_active/reason；`--mode` 預設 None（嘗試 WS，失敗退 REST） |
| `collector_main.py` | 新增 `_update_system_health()`；新增 `ErrorReporter`（scan/forecast/truth 失敗推送） |
| `tools/smoke_test.py` | 新增（9 項回歸測試：city_status/book_state/ev_signals/positions/alert_history/signal_state/system_health/fill_simulator/fee_regression） |

**Part 1：users.json 獨立**
- `data/users.json`：schema_version, allowed_chat_ids, admin_chat_ids, user_details
- 如果不存在 → 自動從 telegram.yaml 的 chat_id bootstrap
- telegram_bot.py 讀 allowed/admin 來自 users.json，不再讀 telegram.yaml 的 list fields
- `UserManager` 類（load/save 原子寫入，bootstrap_from_chat_id，get_allowed，get_admins）

**Part 2：統一 _system_health.json**
- `data/_system_health.json`：各進程各自的 key，共讀共寫，不互相覆蓋
- signal_main：每輪成功/失敗後寫；啟動時寫
- collector_main：每步完成後寫；啟動時寫
- telegram_bot：啟動時寫；主選單回調時更新 last_callback_utc

**Part 3：錯誤自動推送 (ErrorReporter)**
- 10 分鐘冷卻（同一 error_type 不重複推）
- signal_main：consecutive_failures >= 3 → 推；WS > 5 分鐘無事件 → 推
- collector_main：scan/forecast/truth 任一步驟失敗 → 推

**Part 4：12B-2 WS/REST 自動切換**
- run_ws_mode 內：books_in_memory 為空（WS down）→ 自動 REST fallback（08 + 11 json）
- `--mode` 預設 None：嘗試 WS，WS 啟動失敗 → 切換 REST
- `--mode ws`：強制 WS，失敗直接報錯退出
- `_signal_state.json` 新增：ws_fallback_active, ws_fallback_reason

**Part 5：關閉前 flush**
- SIGTERM/SIGINT → position_mgr.flush_edges(0) + _update_system_health("stopped")
- BSM flush 在 run_ws_mode 末尾（已有）

**Part 6：smoke_test.py**
- `tools/smoke_test.py`：`python tools/smoke_test.py [-v]`
- 9 項測試，全 PASS 才退 0

**驗收結果**：
- [x] telegram_bot.py 語法 OK；UserManager 類完整
- [x] signal_main.py 語法 OK；ErrorReporter + shutdown hooks + 12B-2 + health
- [x] collector_main.py 語法 OK；ErrorReporter + health
- [x] tools/smoke_test.py 語法 OK；9 項測試齊全
- [x] `_setup_alert_engine()` 改回傳 (engine, telegram_sender) tuple
- [x] `_write_signal_state()` 新增 ws_fallback_active / ws_fallback_reason
- [x] `--mode` 預設 None（不帶 --mode → 嘗試 WS，失敗退 REST）

**決策紀錄**：
- 2026-04-09：12B-2 放入 STEP 14A（而非 STEP 14 部署後）。理由：在本機測試階段就需要 fallback 能力，等部署再做已太晚。
- 2026-04-09：UserManager 只做讀寫抽象，不做 in-memory cache。Bot 每次需要時 reload users.json（檔案小，I/O 可忽略）。
- 2026-04-09：ErrorReporter 直接使用 TelegramSender.send_message()，chat_id 為 telegram.yaml 預設（alert 推送目標）。admin 與 alert target 通常一致，若以後需要分離可獨立擴充。
- 2026-04-09：_update_system_health 每個進程各自 atomic read-modify-write，無 file lock。多進程並發極少（三個進程各有自己的 key），重疊寫入機率極低且代價是一次 write 遺失（可接受）。

---

### STEP 14B：部署阿里雲 ⬜

pm-collector.service + pm-signal.service（含 telegram_bot）

---

### STEP 15：議題擴充框架 ⬜

只確保 metric_type 全程傳遞，不實作多 metric。

---

## 8. 命名規範

| 舊名 | 新名 | 原因 |
|------|------|------|
| D1 / D1-only | forecast_source + lead_day | 去歧義 |
| fair_yes_price | naive_fair_yes | 不是可執行價 |
| is_finalized | truth_status | 語義精確 |
| signal（混用） | signal_status + signal_action | 狀態與決策分離 |

---

## 9. Stale / Error 降級規則

**`signal_status` 是系統狀態，`signal_action` 是交易建議。兩者不可混為一談。**

| signal_status | 條件 | signal_action | 優先序 |
|---------------|------|---------------|--------|
| `no_price` | 根本沒有任何價格 | `SUPPRESSED` | 1（最高）|
| `book_incomplete` | 有價格但 bids/asks 不完整（book_complete=false）| `SUPPRESSED` | 2 |
| `stale_price` | price_age > 300s | `SUPPRESSED` | 3 |
| `fee_unknown` | 非 Weather 類或 fee 不明 | `SUPPRESSED` | 4 |
| `active` | 所有數據正常 | `BUY_YES` / `BUY_NO` / `NO_TRADE` | 5（最低優先）|
| `token_mismatch` | token_id 對不上（預留）| `SUPPRESSED` | — |
| `model_stale` | 模型 > 7 天未更新（預留）| 顯示 warning，可保留 action | — |

---

## 10. 測試要求

| STEP | 測試 |
|------|------|
| 5 | fee regression（3 筆驗算）+ stale 降級 + fee_status known/unknown |
| 6 | orderbook 完整 + 並行速度 |
| 7 | 城市狀態轉移 |
| 8 | backfill 不阻塞 signal（decoupling） |
| 9 | 三模式結果不同 + sweet_spot 正確 |
| 10 | stale 不推送 + cooldown |
| 11 | Bot 只讀不算 |
| 12 | book reconstruction（snapshot + events） |

---

## 11. 遷移策略

London + Paris 現有流程不可破壞：
- ev_signals.csv 只新增欄位不刪
- market_prices.csv 保留
- 01_main.py 保留
- data/ 新增子目錄不動舊結構
- config/ 新增欄位保留舊欄位

---

## 12. 變更日誌

| 日期 | 版本 | 變更 |
|------|------|------|
| 2026-04-07 | v1 | 初版 |
| 2026-04-07 | v2 | 加 STEP 分版、collector/signal 分離、stale handling |
| 2026-04-08 | v3 | 整合 GPT v3 審核 + fee 驗證 |
| 2026-04-08 | v4 | fee 爭議定案（0.025 正確，數學驗證）+ GPT 6 項建議採納：signal_status/action 分離、fee_mode/basis 命名、ready 門檻暫定、共享輸出契約、book_state 加 debug 欄位、STEP 5 測試補齊 |
| 2026-04-08 | v4.1 | STEP 5 完成：trading_params.yaml 加入 fee_mode/basis/maker/rebate；11_ev_engine.py 加入 signal_status/signal_action 分離、price_age 檢測、fee_status、book_source；fee regression 3 筆全 PASS；live pipeline 驗證通過 |
| 2026-04-08 | v4.2 | STEP 6 完成：08 改用 ThreadPoolExecutor（10 線程）115s→18.9s；新增 book_state JSON 輸出（110 個）；11 改為三層 fallback（book_state > CSV > manual），price_age 改用 snapshot_fetch_time_utc 精確計算 |
| 2026-04-08 | v4.3 | STEP 7 完成：新增 12/13/14；Part 0 前置修正（07 per-station fallback、08 book_complete+fetch_duration_ms、11 signal_status 新增 no_price 狀態）；01_main.py 整合 city_status.json（live mode 只跑 ready 城市，London/Paris 自動偵測）|
| 2026-04-08 | v4.4 | STEP 8 完成：新增 collector_main.py / signal_main.py；Part 0 前置修正（bootstrap() 移到 13、city_status.json 新增 schema_version/last_error/build_time 欄位、08 verify_book_csv_consistency、3.2 共享契約補全 per-city 路徑）|
| 2026-04-08 | v4.5 | STEP 9 完成：新增 _lib/fill_simulator.py（三模式吃單模擬器 + regression test）；11 整合（22 個深度欄位）；08/09/10/11 原子寫入；signal_main 共享契約補齊；trading_params.yaml 新增 depth_fixed_usd/ema_alpha/ema_enabled |
| 2026-04-08 | v4.6 | STEP 10 完成：新增 15_alert_engine.py（AlertEngine + TelegramSender + alert_history 日切檔）；11 run() 改為回傳 (bool, list[dict])；signal_main 改用 in-process ev_engine + 接上 AlertEngine；trading_params.yaml 新增 alert_* 區段；config/telegram.yaml 模板（gitignored）|
| 2026-04-08 | v4.7 | STEP 11 完成：新增 telegram_bot.py（WeatherSignalBot 完整 menu system）+ _lib/signal_reader.py（SignalDataReader）；STEP 10 補丁（has_depth suppress、alert_key、allowed/admin chat ids）；signal_main 新增 refresh flag + _signal_state.json 原子寫入 |
| 2026-04-09 | v4.8 | STEP 12A 完成：新增 08b_price_stream.py（WS ingestion）+ 08c_book_state.py（OrderBook + BookStateManager）；STEP 11 補丁（refresh flag 改 JSON + last_refresh_completed_utc + cb_refresh 傳 chat_id）；12B（signal_main 接 WS）待做 |
| 2026-04-09 | v4.9 | STEP 12B-1 完成：signal_main 新增 --mode ws（run_ws_mode asyncio 主循環）；11_ev_engine 新增 book_source="memory" / books_in_memory 參數；12A 補丁（price_change BUY/SELL 映射修正、best_bid_ask 忽略、flush docstring、get_book()、_bootstrap_done Event）；_signal_state.json 新增 price_mode/ws_connected/last_ws_event_utc；預設仍是 rest 模式 |
| 2026-04-09 | v5.0 | STEP 12 標記 ✅（12B-2 延後至 STEP 14）；STEP 13 完成：新增 16_position_manager.py；15_alert_engine 實作 check_exits()+process_exits()（EXIT cooldown 30 分鐘，edge 轉正自動重置）；11_ev_engine out_row 新增 yes_best_bid/no_best_bid；signal_main REST+WS 模式接入 _update_positions()；signal_reader 新增 get_open_positions/get_closed_positions/get_position；telegram_bot 新增持倉頁+進場/平倉 ConversationHandler |
| 2026-04-09 | v6.0 | STEP 14A 系統收尾完成：telegram_bot 新增 UserManager（users.json 獨立）；三進程共寫 _system_health.json；新增 ErrorReporter（signal/collector 失敗推 Telegram，10 分鐘冷卻）；signal_main 新增 SIGTERM/SIGINT shutdown hooks + 12B-2 WS→REST fallback；--mode 預設 None（嘗試 WS，失敗退 REST）；新增 tools/smoke_test.py（9 項回歸測試） |
| 2026-04-09 | v6.1 | 任務 1：telegram_bot cb_city_signals 改為 inline 按鈕清單（合約可點擊），加合約翻頁（8 筆/頁）；排行頁加 #N 按鈕進詳情；詳情頁「🔙 返回」指向城市信號頁。任務 4：ready 門檻 100→730（約 2 年）+ forecast freshness ≤ 7 天（13/14）；CollectorScheduler 改對齊 UTC 時鐘（scan 06:00 / truth 00:00）；backfill 預設起始日 2023-01-01；trading_params.yaml 新增 ready_min_error_rows / ready_max_forecast_age_days / backfill_start_date |
| 2026-04-09 | v6.2 | Bot UX 重設計（最終版）：全 UI 無 emoji；Reply keyboard 3 按鈕（排行/城市/管理）所有授權用戶；未授權攔截顯示 ID + 請找管理員；管理面板對所有用戶開放（Admin 完整控制，一般用戶只顯示 ID）；Admin CRUD：新增/刪除用戶（ConversationHandler）、所有用戶列表、系統狀態（中文標籤）；排行頁方案 B 排版（空行分隔，城市/溫度粗體，箭頭符號，結算時間）；城市議題頁方案 B 排版（無分頁，全顯示，不顯示 SUPPRESSED/零進場價/零深度），結算只顯示一次；/start 只設 Reply keyboard（發 "Ready"）；新增 tools/send_test_layouts.py（排版測試工具，發 A/B/C 三方案到 Telegram） |
| 2026-04-09 | v6.3 | 防禦性 Bug 修正 20 項（兩批靜態掃描）：07 date filter 傳入 run_step_a/b、round() 修正小數小時、10 missing lead_day guard、12 normalize_city title case、08c last_event_str or ""、16 assert→raise RuntimeError、05 horizon pre-check、03 except pass→log.warning、gen_dashboard bare except→log.debug、01_main fail-closed（live mode 無 ready 城市 sys.exit）；Backfill 逐城市完整鏈路（14_backfill_manager 加入 10_event_probability，城市升 ready 後 signal 立刻可用）；failed 城市 auto-retry（collector 掃描後自動 reset_failed_cities）；部署阿里雲香港 VPS（3 個 screen 進程）；smoke_test 9/9 PASS |
