# 最終設計共識確認 — Claude Code 回答

> 回答時間：2026-04-06
> 依據：qa.md 最終版（三方對齊後）、實際 pipeline 程式

---

## 設計決策定案表

| 項目 | 定案內容 | 狀態 |
|------|----------|------|
| 分位數點數 | 19 點（q05~q95 每 5%） | ✅ 定案 |
| Empirical key | (city, model, lead_bucket, month, local_hour_bucket) + 四層 fallback | ✅ 定案 |
| 09/10 程式 | 09 保留 analysis tool，10 移 experimental | ✅ 定案 |
| 資料切分 | Walk-forward only，不用 random split | ✅ 定案 |
| error 定義 | `error_temp = observed_temp - forecast_temp`（signed） | ✅ 定案 |
| timezone | UTC 為 join 主鍵，local 用於 settlement 計算 | ✅ 定案 |

---

## ~~Blocker：Settlement Truth~~ → 已解決，不是問題

**決定：Polymarket 溫度結算 ≈ A 或 C，直接用 A/C 當 ground truth。**

不需要另外對照合約、不需要 `market_settlement_mapping.csv`。

核心邏輯變成：

```
D 的預報  →  預測未來的 A 或 C 溫度
error     =  A（或 C）- D
分位數模型  →  得到溫度分布
p(event)  →  對比 Polymarket 賠率  →  找 edge  →  交易
```

Settlement truth = A 或 C，已有資料，pipeline 可直接跑。

---

## 優化建議 1：feature 放哪裡

**決定：採用 Codex 版本（feature 放 view，不放 raw table）**

```
raw_forecast_table  ← 純資料，immutable（不含衍生 feature）
settlement_table    ← 純結算紀錄，immutable
model_feature_view  ← 動態 feature 層（month, hour_bucket 等從這裡衍生）
```

**原因：** feature 寫死在 raw table → 未來改 feature 要重跑整個 pipeline，無法做不同組合實驗。

---

## 優化建議 2：A 和 C 都保留，不要只選一個

**決定：同時保留 observed_A 和 observed_C**

```
D vs A → error_A → model_A → Brier_A（對 settlement）
D vs C → error_C → model_C → Brier_C（對 settlement）
```

比哪個最接近 settlement，用數據決定，不要事先選死。

**原因：** 一開始選錯 observed source → 整個系統 bias，且很難發現。

---

## 優化建議 3：OU/AR 要按 lead_bucket 分組

**決定：每個 (city, model, lead_bucket) 各建一條 AR**

```python
# 不是這個（舊版）：
for city, model:
    fit_AR(settlement_error)

# 是這個（正確版）：
for city, model, lead_bucket:
    fit_AR(settlement_error_at_this_bucket)
```

**原因：** lead=24h 和 lead=168h 的誤差結構完全不同（物理上就不一樣），用同一條 AR 是錯的。

---

## 現在可以開始寫的三支程式（spec 已定案）

### 程式 1：Data Cleaning

- join D + A + C snapshot（以 `city + target_time_utc` 為 key）
- 建立 `error_A = observed_A - forecast_temp`
- 建立 `error_C = observed_C - forecast_temp`
- 建立 `lead_bucket`
- 輸出：`raw_forecast_table`、`settlement_table`

### 程式 2：Model Engine

- 輸入：`raw_forecast_table`
- 對每個 (city, model, lead_bucket, month, local_hour_bucket) 建分位數模型
- 輸出：`quantile_table.csv`，每列 = 一組 key，欄位 = q05~q95（19 點）
- 需實作 4 層 fallback

### 程式 3：Walk-forward + Brier

- 輸入：`quantile_table.csv` + settlement truth
- 流程：`distribution → p(event) → Brier`
- 驗證方式：rolling window walk-forward
- 附加：bootstrap CI 估計穩定度

---

## 現在的真實進度

| 層 | 狀態 |
|----|------|
| Data pipeline（A/C/D 抓資料） | ✅ 完成 |
| A/C snapshot 資料存在 | ✅ 已解決（`08_snapshot/A/` 和 `08_snapshot/C/` 有資料） |
| `01_main.py` 呼叫 A/C snapshot | ✅ 已修復（Step 3b/3c） |
| Skip 邏輯（已有資料不重抓） | ✅ 已加入 A/C/D 三個 `_lib` |
| Data schema 設計 | ✅ 完成 |
| Modeling design | ✅ 完成 |
| Validation design | ✅ 完成 |
| Settlement truth 確認 | ✅ 已解決（Polymarket 結算溫度 ≈ A 或 C） |

**所有 blocker 已清除，可以進入實作階段。**

**下一步：寫 Data Cleaning 程式（join D + A + C，建 error table）。**
