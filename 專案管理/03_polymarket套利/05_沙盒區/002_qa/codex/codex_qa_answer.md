# Codex 回答：依新版 QA 修正後的結論

依據：
- `專案管理\03_polymarket套利\05_沙盒區\002_qa\qa.md`
- `專案管理\03_polymarket套利\04_主專案程式\001_polymarket` 的實際檔案
- 我剛剛對 `08_snapshot_main.py --sources A,C` 的實測結果

---

## 一、直接結論

### 1. QA 裡「A/C snapshot 資料不存在」這個問題

**已解。**

A 與 C snapshot 都已經找到，而且我也實際跑成功過。

實際檔案：
- A:
  - `04_主專案程式\001_polymarket\08_snapshot\A\London\snapshot_batch__2026-04-01__2026-04-02__h24.csv`
- C:
  - `04_主專案程式\001_polymarket\08_snapshot\C\London\snapshot_batch__2026-04-01__2026-04-02__h24.csv`

所以這句：

> 唯一剩餘 blocker：A/C snapshot 資料不存在

現在應該視為：

> **過時，不能再當 blocker。**

---

## 二、這一輪應該採用的工作假設

你現在定的方向是：

- 先不要管 Polymarket 合約
- 先做 `D -> A`、`D -> C` 的誤差建模
- 也就是先把問題定成：
  - `forecast_temp = D`
  - `observed_temp = A 或 C`
  - `error_temp = observed_temp - forecast_temp`

我認同這個切法。

### 這代表什麼

這一輪的目標不是：
- `forecast -> settlement -> payout`

而是：
- `forecast(D) -> observed(A/C) -> error distribution`

也就是先建立：
- D1 對 A 的誤差分布
- D1 對 C 的誤差分布
- D2 對 A 的誤差分布
- D2 對 C 的誤差分布

這個階段：
- **不需要把 Polymarket settlement mapping 當 blocker**

---

## 三、A / C / D 的正確語意

### D
- `D` 是 forecast
- 每列代表：
  - 在 `snapshot_time` 當下
  - 對未來某個 `target_time`
  - 所做出的逐小時溫度預測

### A
- `A` 是 station observation truth
- 每列代表：
  - 對應某個 `target_time`
  - 該小時實際觀測到的溫度

### C
- `C` 是 ERA5 reanalysis truth / baseline
- 每列代表：
  - 對應某個 `target_time`
  - 該小時的事後重建真值

### 一句話版本

- `D = 預測幾小時後的溫度`
- `A/C = 那些 target hour 真實發生的溫度`

---

## 四、目前已確認的實際狀態

### D snapshot

目前 `08_snapshot/D` 已有資料，對應：
- `6` 城
- `2` 模型
- `h168`

### A snapshot

我實測成功後，已有：
- `08_snapshot/A/London/snapshot_batch__2026-04-01__2026-04-02__h24.csv`

對應 log：
- `logs/08_snapshot/fetch_a/job_summary.csv`
- `job_status = success`
- `rows_written = 48`

### C snapshot

我實測成功後，已有：
- `08_snapshot/C/London/snapshot_batch__2026-04-01__2026-04-02__h24.csv`

對應 log：
- `logs/08_snapshot/fetch_c/job_summary.csv`
- `job_status = success`
- `rows_written = 48`

---

## 五、所以現在真正剩下的不是「有沒有 A/C」

真正剩下的是：

### 1. coverage 尚未補齊

現在 A/C 已經不是不存在，而是：
- **存在**
- **可跑**
- 但目前只有部分 scope

目前我確認到的 A/C scope：
- `London`
- `2026-04-01 ~ 2026-04-02`
- `h24`

而 D 現有 scope 更大：
- `6 城`
- `2 模型`
- `2026-01-01 ~ 2026-04-05`
- `h168`

所以你現在真正要補的是：
- 把 A/C 補到與 D 相同 scope

### 2. full backfill

若目標是做完整的 `raw_forecast_table`，那要對齊：
- 同城市集
- 同日期範圍
- 同 horizon
- 同 `target_time_utc`

---

## 六、關於 Polymarket settlement 的立場修正

舊版回答把：
- `market_settlement_mapping.csv`

當成當前 blocker。

這在你現在的新前提下，**應該修正**。

### 修正後的立場

#### 對當前這一輪
- **不是 blocker**
- 因為你現在先做的是氣象誤差模型：
  - `D -> A/C`

#### 對未來交易層
- 仍然是重要議題
- 但那是下一層問題

所以比較正確的說法是：

> `market_settlement_mapping` 不是這一輪 D->A/C 誤差建模的 blocker，  
> 但在未來要進入事件機率 / payout / 交易驗證時，仍然需要補上。

---

## 七、bucket 現在怎麼理解

你剛剛提到 bucket，這裡用最直白方式說：

bucket 的作用是：
- 把連續條件分組
- 避免樣本太稀疏

例如：

### lead bucket
- `1-24`
- `25-48`
- `49-72`
- `73-96`
- `97-120`
- `121-144`
- `145-168`

### local hour bucket
- 可以先做：
  - `0-5`
  - `6-11`
  - `12-17`
  - `18-23`

或依你既有設計保留更細分桶。

你現在知道 bucket 的核心意思就夠了：
- **不是為了理論漂亮**
- **是為了讓每格有樣本、能建模**

---

## 八、這一輪真正的主線任務

如果照你現在的新共識，主線應該變成：

### Step 1
把 A/C 補齊到和 D 相同 scope

### Step 2
按 `target_time_utc` 對齊：
- D forecast
- A observed
- C observed

### Step 3
建立兩套誤差表：
- `error_A = A - D`
- `error_C = C - D`

### Step 4
再去比較：
- 哪個 truth 更穩
- 哪個模型更準

這樣才是現在最合理的開工順序。

---

## 九、最終判定

### 已解
- `A/C snapshot 不存在`

### 未解
- `A/C snapshot coverage 尚未補齊`
- `D -> A/C` full-scope join 尚未完成

### 暫時不列為 blocker
- `market_settlement_mapping`

原因：
- 你這一輪不是在做交易結算映射
- 而是在做 D 對 A/C 的誤差建模

---

## 十、一句話總結

**新版 QA 裡把「A/C snapshot 不存在」當 blocker，現在應該拿掉。這一輪正確主線是：先把 A/C 補齊，做 `D -> A/C` 的 full-horizon 誤差建模；Polymarket settlement mapping 留到下一層。**

