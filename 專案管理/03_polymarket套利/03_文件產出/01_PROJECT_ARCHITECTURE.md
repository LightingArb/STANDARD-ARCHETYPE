# Polymarket 天氣交易動態離場系統 — 專案架構與設計文件

> **版本**：v1.9（費用公式修正為天氣市場實際公式：fee_rate=0.025, exponent=0.5）
> **日期**：2026-04-04
> **目標**：建立一套可回測、可落地的 Polymarket 天氣預測市場交易系統，核心解決「進場後何時離場」的動態決策問題。

---

## 一、系統總覽

### 1.1 五層架構

```
┌─────────────────────────────────────────────┐
│  Layer 0 ─ Edge Validation（Phase 0）        │
│  你的 pₜ 有沒有比市場更準？                    │
│  → go / no-go gate                          │
├─────────────────────────────────────────────┤
│  Layer 1 ─ Probability Engine               │
│  溫度分布 → 事件機率 pₜ → 持有價值 Cₜ        │
├─────────────────────────────────────────────┤
│  Layer 2 ─ Market Execution Layer           │
│  orderbook → 可成交淨價格 Aₜ / Lₜ           │
├─────────────────────────────────────────────┤
│  Layer 3 ─ Decision Engine                  │
│  Entry: Kelly sizing                         │
│  Exit:  Lₜ ≥ Cₜ ?                           │
├─────────────────────────────────────────────┤
│  Layer 4 ─ Backtest & Validation            │
│  5 策略比較 + Oracle 上界                     │
└─────────────────────────────────────────────┘
```

### 1.2 核心公式（第一版）

```
離場規則：Sell if Lₜ ≥ Cₜ

其中：
  pₜ = P(event | ℱₜ)                    ← 機率引擎輸出
  Cₜ = pₜ · exp(−rτ)                    ← 持有價值（YES share，時間收益折現版）
  Cₜ = (1−pₜ) · exp(−rτ)               ← NO share 版本
  Lₜ = bid_net − fees                   ← 現在可成交淨賣出價值

  時間收益折現的語意：
    不是「持有有成本」，而是「如果現在釋放資金，剩餘時間內可再利用的要求收益率」。
    r 越高 → 持有價值折越多 → 越傾向提早賣出。
    r = 0 → 不折現 → 純比 pₜ 和 Lₜ。

進場規則：Buy if Cₜ − Aₜ ≥ min_edge（如 0.03）

其中：
  Aₜ = 現在可成交的買入淨成本（ask + fee + slippage）
  進場和離場統一口徑：都用可成交淨價格，不用畫面 mid price

Kelly sizing（用 raw probability，不用折現後的 Cₜ）：
  Cₜ_raw = pₜ（YES）或 (1−pₜ)（NO）
  full_kelly = (Cₜ_raw − Aₜ) / (1 − Aₜ)
  actual_size = full_kelly × kelly_fraction × bankroll

單位定義（全專案統一）：
  τ = hours to settlement（小時）
  r = per-hour reinvestment yield / hurdle rate（每小時資金再利用收益率）
  Lₜ, Aₜ, Cₜ 均為 per-share USD 值
```

---

## 二、目錄結構

```
polymarket-exit-system/
│
├── README.md                          # 專案說明
├── PROJECT_ARCHITECTURE.md            # 本文件
├── config/
│   ├── settings.yaml                  # 全域設定（r, min_edge, kelly_fraction 等）
│   ├── markets.yaml                   # 市場定義（合約條件、結算規則、費率）
│   └── data_sources.yaml              # 資料來源設定（API endpoints, 氣象站）
│
├── src/
│   ├── __init__.py
│   │
│   ├── phase0_edge_validation/        # Phase 0: Edge 驗證
│   │   ├── __init__.py
│   │   ├── collect_historical.py      # 收集歷史市場價格 + 結算結果
│   │   ├── model_probability.py       # 算你的歷史 pₜ
│   │   ├── market_probability.py      # 算市場隱含機率
│   │   ├── compare_edge.py            # Brier score / log loss 比較
│   │   └── report.py                  # 輸出 Phase 0 報告
│   │
│   ├── data/                          # 資料管理層
│   │   ├── __init__.py
│   │   ├── weather_data.py            # 氣象資料抓取與清洗
│   │   ├── polymarket_data.py         # Polymarket 歷史價格 / orderbook
│   │   ├── schema.py                  # 資料 schema 定義
│   │   └── alignment.py              # 時間軸 / 口徑對齊
│   │
│   ├── probability_engine/            # Layer 1: 機率引擎
│   │   ├── __init__.py
│   │   ├── error_model.py             # e = y - f 誤差計算
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   ├── empirical.py           # Baseline A: 分桶 empirical 分布
│   │   │   ├── quantile_regression.py # Baseline B: 分位數回歸
│   │   │   └── ou_ar.py              # Baseline C: OU / AR(1)
│   │   ├── distribution.py            # 溫度分布物件（統一介面）
│   │   ├── event_probability.py       # 溫度分布 → 事件機率 pₜ
│   │   ├── hold_value.py             # pₜ → Cₜ（持有價值）
│   │   └── calibration.py            # pₜ 校準（isotonic / Platt-like，第二版啟用）
│   │
│   ├── market_engine/                 # Layer 2: 可成交價格
│   │   ├── __init__.py
│   │   ├── orderbook.py               # orderbook 讀取與解析
│   │   ├── liquidation_value.py       # 計算 Lₜ（賣出淨價值：bid − fee − slippage）
│   │   ├── acquisition_value.py       # 計算 Aₜ（買入淨成本：ask + fee + slippage）
│   │   ├── fees.py                    # Polymarket 費率模型
│   │   └── spread_analysis.py        # spread / depth 統計分析
│   │
│   ├── decision_engine/               # Layer 3: 決策引擎
│   │   ├── __init__.py
│   │   ├── entry.py                   # 進場決策 + Kelly sizing
│   │   ├── exit.py                    # 離場決策：Lₜ ≥ Cₜ
│   │   └── position.py               # 倉位狀態管理
│   │
│   └── backtest/                      # Layer 4: 回測
│       ├── __init__.py
│       ├── engine.py                  # 回測引擎主體（walk-forward）
│       ├── strategies.py              # 5 個比較策略定義
│       ├── oracle.py                  # Oracle 上界策略
│       ├── metrics.py                 # 績效指標計算
│       ├── probability_metrics.py     # 機率校準指標（Brier, CRPS, PIT）
│       └── report.py                 # 回測報告產出
│
├── notebooks/                         # 探索性分析
│   ├── 01_eda_error_distribution.ipynb
│   ├── 02_model_comparison.ipynb
│   ├── 03_backtest_results.ipynb
│   └── 04_parameter_sensitivity.ipynb
│
├── tests/
│   ├── test_probability_engine.py
│   ├── test_market_engine.py
│   ├── test_decision_engine.py
│   └── test_backtest.py
│
└── data/
    ├── raw/                           # 原始資料
    ├── processed/                     # 清洗後資料
    └── results/                       # 回測結果
```

---

## 二之一、統一命名規範與資料 Schema

### 命名規範（全專案統一）

| 符號 | 欄位名 | 意義 |
|------|--------|------|
| f | `forecast_temp` | 預測溫度值 |
| y | `observed_temp` | 真實觀測溫度值 |
| e | `error_temp` | 誤差 = observed − forecast = y − f |

**規則：永遠三個都存。** e 是衍生欄位，不取代 f 和 y。

原因：只存差值會丟失背景資訊（這筆誤差是在預測 10°C 還是 35°C 時發生的、白天還是晚上、高溫區還是低溫區）。

### 最小原始資料表（weather_observations）

```
forecast_time          datetime     # 你下這個預測的時間
target_time            datetime     # 這筆預測對應的未來時間
lead_hours             float        # 幾小時 ahead
forecast_temp          float        # f：當時對 target_time 的預測溫度
observed_temp          float        # y：target_time 的真實觀測溫度
error_temp             float        # e = y - f（衍生欄位）
current_obs_temp       float        # 下預測當下的實況溫度（常見有用特徵）
station_id             str          # 氣象站
```

### 市場資料表（market_snapshots）

```
timestamp              datetime     # 快照時間
market_id              str          # Polymarket 市場 ID
contract_rule          str          # 結算條件（如 'above_30', 'between_21_22'）
yes_price              float        # YES mid price（或 last trade）
no_price               float        # NO mid price
yes_best_bid           float        # YES 最佳買價
yes_best_ask           float        # YES 最佳賣價
no_best_bid            float        # NO 最佳買價
no_best_ask            float        # NO 最佳賣價
yes_bid_depth_1        float        # YES 第一檔買量
yes_ask_depth_1        float        # YES 第一檔賣量
no_bid_depth_1         float        # NO 第一檔買量
no_ask_depth_1         float        # NO 第一檔賣量
spread                 float        # yes_best_ask - yes_best_bid
settlement_time        datetime     # 結算時間
settlement_outcome     int | null   # 結算結果：1 = YES, 0 = NO, null = 未結算
```

### 交易紀錄表（trades）

```
trade_id               str
market_id              str
side                   str          # 'YES' or 'NO'
action                 str          # 'BUY' or 'SELL'
size                   float        # 股數
price                  float        # 成交價
fee                    float        # 手續費
net_amount             float        # 淨金額
timestamp              datetime
p_t_at_trade           float        # 交易當下你的主觀機率
C_t_at_trade           float        # 交易當下的 Cₜ
L_t_at_trade           float        # 交易當下的 Lₜ（若為賣出）
A_t_at_trade           float        # 交易當下的 Aₜ（若為買入）
```

### 模型輸入依目的分流

| 模型類型 | 輸入特徵（X） | 目標值（Y） | 輸出 |
|----------|---------------|-------------|------|
| 分位數回歸（誤差版） | forecast_temp, current_obs_temp, lead_hours, hour, month, station | error_temp (e) | 誤差的分位數 → 加回 f 得溫度分布 |
| 分位數回歸（直接版） | forecast_temp, current_obs_temp, lead_hours, hour, month, station | observed_temp (y) | 溫度的分位數（直接就是溫度分布）|
| OU / AR(1) | 固定 lead_time 下的 error_temp 時間序列 | e_{t+1} | 條件常態分布 → 加回 f 得溫度分布 |
| Empirical 分桶 | (month, hour_bucket, lead_time_bucket) → 桶 | 歷史 error_temp 集合 | empirical CDF → 加回 f |

---

### 3.1 Phase 0 — Edge Validation

**目的**：在投入任何工程資源之前，先確認你的機率估計有沒有比市場更準。

**設計邏輯**：

```python
# compare_edge.py 核心邏輯

def validate_edge(historical_events):
    """
    輸入：歷史事件列表，每筆包含：
      - timestamp
      - your_probability (你的 pₜ)
      - market_probability (市場 mid price)
      - actual_outcome (0 or 1)

    輸出：
      - your_brier_score
      - market_brier_score
      - difference + statistical significance
      - calibration_curve_data
      - go / no-go 判定
    """
    # Brier Score = mean((p - outcome)²)
    your_brier = mean((your_prob - outcome) ** 2)
    market_brier = mean((market_prob - outcome) ** 2)

    # 若你的 Brier 沒有顯著低於市場 → no-go
    # 顯著性可用 paired t-test 或 bootstrap
```

**go / no-go 判準**：
- 你的 Brier score 需顯著低於市場（p-value < 0.05）
- 或 calibration curve 顯示市場有系統性偏差而你沒有
- 如果差異不顯著 → 停止，先改善機率模型

**Phase 0 硬規則（不可違反）**：

> 所有 your_probability（你的 pₜ）必須由當時點可得資訊產生，不可使用未來資料、不可使用全樣本訓練後回填的預測結果。
>
> Phase 0 一律使用 walk-forward / rolling out-of-sample 評估：
> - train window → fit model
> - next test window → predict（產生 pₜ）
> - 滑動重複
>
> 否則 edge 驗證結果無效。

**Phase 0 分層理解**：

- **Phase 0A — forecast edge**：比較你的 pₜ 與市場 mid probability 的預測品質（Brier / log loss / calibration）。這回答「你的模型有沒有比市場更準」。
- **Phase 0B — tradable edge**：在納入 Aₜ（買入淨成本）、Lₜ（賣出淨值）、fee、spread、slippage 後，你的優勢是否仍然足以支撐實際交易。

Phase 0A 通過，不代表 Phase 0B 一定通過。有些情況下你比市場準，但扣掉交易成本後邊際優勢就消失了。

---

### 3.2 Layer 1 — Probability Engine

**資料流**：

```
原始預報 f → 歷史誤差 e = y - f → 誤差模型 → 溫度分布 → 事件機率 pₜ → 持有價值 Cₜ
```

**五個核心函式**：

#### 函式 1：predict_distribution(features, tau)

```python
def predict_distribution(features: dict, tau: float) -> TemperatureDistribution:
    """
    輸入：
      features = {
          'forecast_temp': 28.5,        # f：當前預報值
          'current_obs_temp': 27.0,     # 當下實況溫度
          'month': 7,
          'hour': 14,
          'station_id': 'london_heathrow',
          'lead_hours': tau              # 距結算剩餘小時
      }

    輸出：TemperatureDistribution 物件，包含：
      - quantiles: {0.05: 25.1, 0.10: 25.8, ..., 0.95: 32.4}
      - mean: 28.3
      - std: 2.1
      - pdf(x): 連續密度函數（若有）

    設計重點：
      1. 統一介面：不管底下是 empirical / QR / OU，輸出格式一樣
      2. τ 已經內含：模型本身會根據不同 lead_time 輸出不同寬度的分布
         → 這就是為什麼不需要手工 g(τ)
    """
```

#### 函式 2：event_probability(distribution, contract_rule)

```python
def event_probability(
    dist: TemperatureDistribution,
    contract_rule: ContractRule
) -> float:
    """
    輸入：
      dist: 溫度分布
      contract_rule: 合約結算條件
        例如 ContractRule(type='above', threshold=30.0)
        或   ContractRule(type='between', low=21.0, high=22.0)

    輸出：pₜ（事件機率，0 到 1 之間）

    計算方式：
      - 'above':  pₜ = 1 - CDF(threshold)
      - 'below':  pₜ = CDF(threshold)
      - 'between': pₜ = CDF(high) - CDF(low)

    若只有離散分位數（沒有連續 CDF），用線性插值：
      在相鄰分位點之間做線性插值得到近似 CDF
    """
```

#### 函式 3：hold_value(p_t, tau, side, r)

```python
import math

def hold_value(
    p_t: float,
    tau: float,
    side: str,           # 'YES' or 'NO'
    r: float = 0.0       # per-hour reinvestment yield / hurdle rate
) -> float:
    """
    輸入：
      p_t: 事件機率
      tau: 剩餘小時
      side: 你持有的是 YES 還是 NO
      r: 資金再利用收益率（每小時）

    輸出：Cₜ（持有到結算的時間收益折現期望值）

    公式：
      YES: Cₜ = pₜ · exp(−rτ)
      NO:  Cₜ = (1 − pₜ) · exp(−rτ)

    語意：不是「持有有成本」，而是「如果現在把資金釋放出來，
    剩餘 τ 小時內可再利用的要求收益率 r 所對應的折現」。

    第一版 r = 0（不折現，純比 pₜ 和 Lₜ）。
    回測時再測 r = 0.0001 / 0.0005 / 0.001 等候選值。
    若 r 敏感度低 → 直接維持 r = 0。

    註：Cₜ 已包含時間收益折現。
    Aₜ 與 Lₜ 僅代表當前可成交淨價格，不再重複折現，以避免 double counting。
    """
    discount = math.exp(-r * tau)
    raw = p_t if side == 'YES' else (1 - p_t)
    return raw * discount
```

#### 三個 Baseline 模型的設計

**Baseline A — Empirical / 分桶分布**：

```python
class EmpiricalErrorModel:
    """
    最簡單版本：
    1. 把歷史 error_temp 按 (month, hour_bucket, lead_time_bucket) 分桶
    2. 每個桶裡就是一組歷史 error_temp 值
    3. 預測時，找到對應桶，直接用那些歷史誤差當 empirical distribution
    4. future_temp = forecast_temp + error_sample

    優點：零假設、快速、直覺
    缺點：桶太細時樣本不夠、無法外推
    """
```

**Baseline B — 分位數回歸**：

```python
class QuantileRegressionModel:
    """
    核心做法：
    1. 訓練資料：X = [forecast_temp, current_obs_temp, month, hour, lead_hours, station_id]
                  Y = error_temp (= observed_temp - forecast_temp = y - f)
    2. 對 q ∈ {0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95} 各訓練一個回歸
    3. 預測時輸出各分位點 → 加回 forecast_temp → 組成溫度分布

    實作選擇（由簡到繁）：
      a. sklearn / statsmodels 的 QuantileRegressor（線性）
      b. LightGBM 的 quantile loss
      c. Quantile Regression Forest

    第一版建議用 (a) 線性分位數回歸，夠快、夠穩
    """
```

**Baseline C — OU / AR(1)**：

```python
class OUErrorModel:
    """
    假設誤差 eₜ 遵循 OU 過程：
      de = -θ(e - μ)dt + σdW

    其中 θ 為均值回歸速度，與主系統中的時間收益率 r 無關。

    離散版（AR(1)）：
      e_{t+1} = φ * eₜ + (1-φ) * μ + ε,  ε ~ N(0, σ²_ε)

    從歷史誤差序列估計 φ（= exp(-θΔt)）, μ, σ_ε
    預測 h 步後的條件分布：
      E[e_{t+h}] = μ + φ^h * (eₜ - μ)
      Var[e_{t+h}] = σ²_ε * (1 - φ^{2h}) / (1 - φ²)

    優點：解析解、捕捉均值回歸
    缺點：假設線性高斯
    """
```

---

### 3.3 Layer 2 — Market Execution Layer

#### 函式 4：liquidation_value(orderbook, position_size, fee_rate)

```python
def liquidation_value(
    orderbook: OrderBook,
    position_size: float,
    fee_rate: float,
    side: str               # 'YES' or 'NO'
) -> float:
    """
    計算現在立刻賣出 position_size 股的淨可得金額 Lₜ

    步驟：
    1. 讀取 orderbook 的 bids（買方掛單）
    2. 從最高 bid 開始吃，逐檔累積直到填滿 position_size
    3. 每檔計算：
       gross = price × quantity_filled
       fee = price × fee_rate × (price × (1 - price))^exponent × quantity_filled
       net = gross - fee
    4. 加總所有檔的 net = Lₜ（總淨得）
    5. 若要得每股淨價：Lₜ_per_share = Lₜ / position_size

    若 orderbook 深度不夠填滿 position_size：
      → 回傳可成交部分的 Lₜ + 未成交部分標記為 None
      → 離場決策應考慮「能不能全部賣掉」

    fee 公式（Polymarket weather market，自 2026-03-30 起）：
      fee_per_share = p × fee_rate × (p × (1 - p))^exponent
      其中 fee_rate = 0.025, exponent = 0.5, p = 成交價格
      注意：不要硬編碼，用 API 動態查詢 fee_rate 和 exponent
    """
```

**OrderBook 資料結構**：

```python
@dataclass
class OrderBook:
    bids: list[tuple[float, float]]   # [(price, quantity), ...]  降序
    asks: list[tuple[float, float]]   # [(price, quantity), ...]  升序
    timestamp: datetime
    market_id: str
```

---

### 3.4 Layer 3 — Decision Engine

#### 函式 5：should_exit(L_t, C_t)

```python
def should_exit(L_t: float, C_t: float) -> bool:
    """
    最核心的離場判斷。

    Sell if Lₜ ≥ Cₜ

    就這麼簡單。

    Lₜ = 現在賣掉能拿到的錢
    Cₜ = 模型認為繼續抱到結算值多少（已含時間收益折現）
    """
    return L_t >= C_t
```

#### 進場決策

```python
def acquisition_value(
    orderbook: OrderBook,
    position_size: float,
    fee_rate: float,
    side: str
) -> float:
    """
    計算現在立刻買入 position_size 股的淨成本 Aₜ（對稱於 Lₜ）

    步驟：
    1. 讀取 orderbook 的 asks（賣方掛單）
    2. 從最低 ask 開始吃，逐檔累積
    3. 每檔：cost = price × qty + price × fee_rate × (price × (1-price))^exponent × qty
    4. 加總 = Aₜ（總淨成本）
    5. 每股淨成本：Aₜ_per_share = Aₜ / position_size
    """


def should_enter(
    C_t: float,
    A_t: float,
    min_edge: float = 0.03
) -> bool:
    """
    進場條件：持有價值 − 買入淨成本 ≥ 最低門檻

    C_t = hold_value（pₜ · exp(−rτ) for YES, (1−pₜ) · exp(−rτ) for NO）
    A_t = acquisition_value（ask + fee + slippage）

    統一口徑：進場和離場都用可成交淨價格
    """
    return (C_t - A_t) >= min_edge


def kelly_size(
    C_t_raw: float,
    A_t: float,
    bankroll: float,
    kelly_fraction: float = 0.25   # 1/4 Kelly
) -> float:
    """
    二元市場 Kelly sizing（用淨成本，不用裸價格）

    C_t_raw = pₜ（YES）或 (1−pₜ)（NO），不含時間折現
    A_t = 可成交買入淨成本（ask + fee + slippage）

    full_kelly = (C_t_raw − A_t) / (1 − A_t)
    actual_size = full_kelly × kelly_fraction × bankroll

    kelly_fraction 建議 0.25（四分之一 Kelly）
    """
    if C_t_raw <= A_t:
        return 0.0
    full_kelly = (C_t_raw - A_t) / (1 - A_t)
    return max(0, full_kelly * kelly_fraction * bankroll)
```

---

### 3.5 Layer 4 — Backtest

#### 五個比較策略

```python
class StrategyA_HoldToSettlement:
    """永遠抱到結算，不做盤中離場"""

class StrategyB_ThresholdExit:
    """簡化離場規則：Lₜ ≥ Cₜ = pₜ · exp(−rτ)"""

class StrategyC_EVEntryOnly:
    """只用 EV 進場，進場後不做離場，抱到結算"""

class StrategyD_ImmediateProfit:
    """只要有任何帳面獲利就立刻賣"""

class StrategyE_Oracle:
    """
    事後諸葛策略（理論上界）：
    已知最終結果，回頭算每筆交易的最佳離場時點。

    硬規則：Oracle 只使用歷史上真實可成交價格（bid / net liquidation value），
    不可使用 mid price 或理論最優但不可成交的價格。

    用途：衡量離場優化的天花板有多高。
    不是拿來交易的。
    """
```

#### 回測引擎核心邏輯

```python
def run_backtest(
    strategy: BaseStrategy,
    historical_data: pd.DataFrame,
    train_window: int,       # 訓練視窗（天）
    test_window: int,        # 測試視窗（天）
) -> BacktestResult:
    """
    Walk-forward backtest：
    1. 用 [t - train_window, t] 訓練模型
    2. 用 [t, t + test_window] 測試策略
    3. 滑動視窗往前移
    4. 累積所有測試期間的結果

    每個時間步（如每小時）：
      a. 更新 pₜ（重新跑機率引擎）
      b. 讀取 orderbook → 算 Aₜ（買入淨成本）
      c. 讀取 orderbook → 算 Lₜ（賣出淨值）
      d. 算 Cₜ
      e. 執行策略決策（進場 / 離場 / 持有）
      f. 記錄狀態
    """
```

#### 績效指標

```python
# 機率層指標
def brier_score(predictions, outcomes): ...
def log_loss(predictions, outcomes): ...
def crps(quantile_forecasts, observations): ...
def pit_histogram(cdf_forecasts, observations): ...
def calibration_curve(predictions, outcomes, n_bins=10): ...

# 交易層指標
def realized_pnl(trades): ...
def sharpe_ratio(returns, risk_free=0): ...
def sortino_ratio(returns, risk_free=0): ...
def max_drawdown(equity_curve): ...
def avg_holding_time(trades): ...
def capital_turnover(trades, total_capital): ...
def exit_regret(trades):
    """比較：提前賣出的實際收益 vs 若抱到結算的收益"""
```

---

## 四、設定檔範例

### settings.yaml

```yaml
# ===== 第一版系統設定 =====

# Phase 0
edge_validation:
  min_brier_improvement: 0.01    # pₜ 的 Brier 至少要比市場好這麼多
  significance_level: 0.05

# 機率引擎
probability_engine:
  default_model: "quantile_regression"  # empirical / quantile_regression / ou_ar
  quantiles: [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]
  features:
    - forecast_temp
    - current_obs_temp
    - month
    - hour
    - lead_hours
    - station_id

# 時間收益折現
time_discounting:
  reinvestment_rate: 0.0          # 第一版先設 0（不折現），回測再調
  reinvestment_rate_candidates: [0.0, 0.0001, 0.0005, 0.001]

# 進場決策
entry_decision:
  min_edge: 0.03                 # Cₜ − Aₜ 的最低門檻
  kelly_fraction: 0.25           # 1/4 Kelly
  max_position_pct: 0.10         # 單一市場最大倉位比例

# 市場執行
market_execution:
  default_fee_rate: 0.025        # weather market fee_rate parameter
  default_fee_exponent: 0.5      # weather market exponent parameter
  # 實際公式：fee_per_share = p × fee_rate × (p × (1-p))^exponent
  # 不要硬編碼，用 API 動態查詢
  max_slippage_pct: 0.02         # 超過此滑價不執行

# 回測
backtest:
  train_window_days: 90
  test_window_days: 30
  time_step_hours: 1             # 每小時檢查一次
  strategies: [A, B, C, D, E]
```

---

## 五、開發階段與優先序

### Phase 0（1-2 天）
- [ ] 收集歷史天氣市場的 market price + settlement outcome
- [ ] 用你現有的預報模型算出歷史 pₜ（必須 walk-forward）
- [ ] **Phase 0A**：跑 Brier score / log loss 比較（forecast edge）
- [ ] **Phase 0B**：納入 Aₜ、Lₜ、spread、fee 後，檢查是否仍有 tradable edge
- [ ] 判定 go / no-go

### Phase 1（3-5 天）
- [ ] 建立資料 schema 與時間對齊
- [ ] 實作三個 baseline 機率模型
- [ ] 實作 event_probability + hold_value
- [ ] 用 CRPS / PIT / calibration curve 比較三個模型，並在事件映射後補看 Brier / log loss

### Phase 2（3-5 天）
- [ ] 實作 orderbook 讀取 + liquidation_value
- [ ] 實作 entry（Kelly）+ exit（Lₜ ≥ Cₜ）
- [ ] 建立回測引擎
- [ ] 跑 5 個策略比較

### Phase 3（2-3 天）
- [ ] 分析回測結果
- [ ] r（reinvestment rate）敏感度測試
- [ ] 決定是否需要升級模型（Kalman / Bellman）
- [ ] 產出最終報告

---

## 六、給 Claude Code / Codex 的實作提示詞

以下是你拿去給 AI 寫 code 時可以直接用的 prompt。

### Prompt 1：資料層

```
請幫我建立一個 Python 資料管理模組，結構如下：

統一命名規範（全專案）：
  f = forecast_temp = 預測溫度
  y = observed_temp = 真實觀測溫度
  e = error_temp = y - f = 誤差（衍生欄位）
  三個都要存，不能只存差值。

1. weather_data.py
   - 從指定氣象 API（如 Wunderground 或 Open-Meteo）抓取歷史與即時溫度資料
   - 輸出格式：DataFrame，欄位包含：
     forecast_time, target_time, lead_hours, forecast_temp, observed_temp,
     error_temp (= observed_temp - forecast_temp), current_obs_temp, station_id

2. polymarket_data.py
   - 從 Polymarket REST API 抓取指定市場的歷史成交價、當前 orderbook
   - 輸出格式：
     - 歷史價格：DataFrame with columns [timestamp, market_id, yes_price, no_price, 
       yes_best_bid, yes_best_ask, no_best_bid, no_best_ask,
       yes_bid_depth_1, yes_ask_depth_1, no_bid_depth_1, no_ask_depth_1,
       spread, volume,
       settlement_time, settlement_outcome]
     - Orderbook：OrderBook dataclass with bids/asks as list of (price, quantity) tuples

3. schema.py
   - 定義所有資料物件的 dataclass / Pydantic model
   - 包含：WeatherObservation（含 f, y, e 三欄位）, MarketSnapshot, 
     OrderBook, Trade, Position
   - Trade 要包含 p_t_at_trade, C_t_at_trade, L_t_at_trade, A_t_at_trade

4. alignment.py
   - 把氣象時間軸和市場時間軸對齊到同一個 hourly grid
   - 處理時區轉換（UTC 基準）

技術要求：
- Python 3.11+
- pandas, requests, pydantic
- 所有 API call 加 retry + rate limit
- 每個函式都要 type hint + docstring
```

### Prompt 2：機率引擎

```
請幫我建立一個機率引擎模組 probability_engine/，包含：

統一命名規範：
  f = forecast_temp, y = observed_temp, e = error_temp = y - f

1. error_model.py
   - 計算 e = observed_temp - forecast_temp
   - 輸出 error 時間序列

2. models/empirical.py
   - EmpiricalErrorModel 類別
   - fit(errors, features)：按 (month, hour_bucket, lead_time_bucket) 分桶，存每桶的 error_temp 值
   - predict(features) → TemperatureDistribution：從對應桶取歷史誤差 + forecast_temp 組成分布

3. models/quantile_regression.py
   - QuantileRegressionModel 類別
   - fit(X, y, quantiles=[0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])
     X 包含：forecast_temp, current_obs_temp, month, hour, lead_hours, station_id
     y 為 error_temp
   - predict(X) → TemperatureDistribution（誤差分位數 + forecast_temp = 溫度分布）
   - 第一版用 statsmodels QuantReg（線性），預留切換 LightGBM 的介面

4. models/ou_ar.py
   - OUErrorModel 類別
   - fit(error_series)：從固定 lead_time 的 error_temp 序列估計 φ, μ, σ_ε
   - predict(current_error, lead_time) → TemperatureDistribution
   - 條件分布：N(μ + φ^h(eₜ - μ), σ²(1 - φ^{2h})/(1 - φ²))

5. distribution.py
   - TemperatureDistribution 類別（統一介面）
   - 第一版預設針對單一已定義目標變數（如某時點溫度）；
     若 target 擴展至 daily high / daily low / average temp，需改為更通用的 TargetDistribution 介面
   - 屬性：quantiles dict, mean, std
   - 方法：cdf(x), pdf(x), ppf(p), interval_probability(low, high)
   - 若只有離散分位數，用線性插值

6. event_probability.py
   - event_probability(dist: TemperatureDistribution, rule: ContractRule) → float
   - ContractRule dataclass：type ('above'/'below'/'between'), threshold / low / high

7. hold_value.py
   - hold_value(p_t, tau, side, r) → float
   - YES: Cₜ = pₜ · exp(−rτ)
   - NO:  Cₜ = (1 − pₜ) · exp(−rτ)
   - r = per-hour reinvestment yield / hurdle rate
   - 語意：時間導致的資金再利用收益折現，不是持有成本

技術要求：
- 所有模型繼承統一的 BaseModel(ABC) 介面
- fit / predict 介面一致
- 輸出一律是 TemperatureDistribution
- 資料表保留 forecast_temp, observed_temp, error_temp 三欄位
```

### Prompt 3：市場執行層

```
請幫我建立 market_engine/ 模組：

1. orderbook.py
   - OrderBook dataclass
   - 從 Polymarket API response 解析 bids/asks
   - best_bid(), best_ask(), mid_price(), spread()

2. fees.py
   - calculate_fee(price, fee_rate) → float
   - 公式：fee_per_share = p × fee_rate × (p × (1 - p))^exponent
   - 天氣市場：fee_rate = 0.025, exponent = 0.5
   - 不要硬編碼，用 API 動態查詢 fee_rate 和 exponent

3. liquidation_value.py（賣出端：Lₜ）
   - liquidation_value(orderbook, position_size, fee_rate, side) → LiquidationResult
   - LiquidationResult 包含：
     total_net: float          # 總淨得
     avg_price: float          # 平均成交價
     total_fee: float          # 總手續費
     fills: list[(price, qty)] # 逐檔成交明細
     unfilled: float           # 未能成交的數量（若深度不夠）

4. acquisition_value.py（買入端：Aₜ，與 Lₜ 對稱）
   - acquisition_value(orderbook, position_size, fee_rate, side) → AcquisitionResult
   - AcquisitionResult 包含：
     total_cost: float         # 總淨成本
     avg_price: float          # 平均成交價（含費）
     total_fee: float          # 總手續費
     fills: list[(price, qty)] # 逐檔成交明細
     unfilled: float
   - 邏輯：從 asks 最低價開始吃，每檔
     cost = price × qty + price × fee_rate × (price × (1 - price))^exponent × qty

5. spread_analysis.py
   - 統計歷史 spread 與 depth 分布
   - 分析 spread 與剩餘時間 τ 的關係
```

### Prompt 4：決策引擎

```
請幫我建立 decision_engine/ 模組：

核心原則：進場和離場統一用可成交淨價格，不用畫面 mid price。
單位定義：τ = hours, r = per-hour reinvestment yield, Lₜ/Aₜ/Cₜ = per-share USD

1. entry.py
   - should_enter(C_t, A_t, min_edge) → bool
     進場條件：C_t - A_t >= min_edge
     C_t = hold_value（時間收益折現後的持有期望值）
     A_t = acquisition_value（ask + fee + slippage 的買入淨成本）
   - kelly_size(C_t_raw, A_t, bankroll, kelly_fraction) → float
     C_t_raw = pₜ（YES）或 (1-pₜ)（NO），不含時間折現
     公式：full_kelly = (C_t_raw - A_t) / (1 - A_t)
     回傳 full_kelly × kelly_fraction × bankroll
     加 max_position_pct 上限

2. exit.py
   - should_exit(L_t, C_t) → bool
   - 就是 L_t >= C_t
   - 同時回傳 exit_reason（如 'threshold_met', 'settlement_near' 等）

3. position.py
   - Position dataclass：market_id, side, size, entry_price, entry_time, A_t_at_entry
   - 方法：
     current_liquidation_value(orderbook, fee_rate) → 現在真的賣掉能拿多少（= Lₜ）
     unrealized_pnl(orderbook, fee_rate) → Lₜ − entry_cost
     holding_hours()
   - 不直接用裸 current_bid，統一用可成交淨價格口徑
```

### Prompt 5：回測引擎

```
請幫我建立 backtest/ 模組：

1. engine.py
   - run_backtest(strategy, data, config) → BacktestResult
   - Walk-forward：train_window → test_window → slide
   - 每個 time_step：
     a. 更新 pₜ（機率引擎）
     b. 讀取 orderbook → 算 Aₜ（買入淨成本）
     c. 讀取 orderbook → 算 Lₜ（賣出淨值）
     d. 算 Cₜ
     e. 呼叫 strategy.decide(state) → Action (enter/exit/hold)
     f. 記錄
   - 回測需扣除手續費與滑價

2. strategies.py
   - 5 個策略類別，都繼承 BaseStrategy
   - A: HoldToSettlement
   - B: ThresholdExit（用 Lₜ ≥ Cₜ）
   - C: EVEntryOnly（進場後不離場）
   - D: ImmediateProfit（帳面有賺就賣）
   - E: Oracle（事後知道結果，算理論最佳離場）

3. oracle.py
   - 給定一筆交易的完整價格路徑和最終結果
   - 找出在哪個時點賣出能得到最高淨值
   - 這就是理論上界

4. metrics.py
   - realized_pnl, sharpe, sortino, max_drawdown
   - avg_holding_time, capital_turnover
   - exit_regret（vs hold-to-settlement）

5. probability_metrics.py
   - brier_score, log_loss, crps, pit_histogram, calibration_curve

6. report.py
   - 產出包含所有指標的比較表
   - 策略 A vs B vs C vs D vs E
   - 含圖表（equity curve, calibration plot, holding time distribution）
```

---

## 七、關鍵設計決策紀錄

| # | 決策 | 選擇 | 原因 |
|---|------|------|------|
| 1 | 離場公式形式 | Lₜ ≥ Cₜ（不用手工 g(τ)） | 分位數模型本身已吸收時間不確定性，少一層參數少一層 overfit |
| 2 | 進場公式形式 | Cₜ − Aₜ ≥ min_edge | 進場離場統一用可成交淨價格，不混用 mid price |
| 3 | Kelly 用什麼價格 | 用淨成本 Aₜ，不用裸 ask | 跟真實交易一致 |
| 4 | 時間項公式形式 | 時間收益折現 Cₜ = pₜ · exp(−rτ)，不用 κτ 成本式 | 更符合語意：時間的作用來自資金釋放後的再利用收益，不是單純持有成本 |
| 5 | r 和 τ 的單位 | τ = hours, r = per-hour reinvestment yield | 全專案統一，避免單位混亂 |
| 6 | Kelly fraction | 0.25（1/4 Kelly） | pₜ 有估計誤差，full Kelly 太激進 |
| 7 | 機率模型優先序 | QR > empirical > OU | QR 直接輸出分位數，最接近需求 |
| 8 | 是否做 Bellman | Phase 3 才考慮 | 先驗證簡化規則有沒有效，沒效才升級 |
| 9 | Oracle 策略用途 | 只當上界，用可成交價不用 mid | 衡量離場優化的天花板，避免灌水 |
| 10 | Phase 0 是否可省 | 不可省，且必須 walk-forward | 沒有 edge 的離場公式只是在優化噪音 |
| 11 | 命名規範 | f / y / e 三個都存 | 只存差值會丟失背景資訊 |
| 12 | TemperatureDistribution | 第一版先用，擴展時改名 | 若 target 擴展至 daily high/low，需升級為通用 TargetDistribution |

---

## 八、風險與限制

1. **pₜ 估計誤差**：機率引擎不可能完美，所有下游決策都建立在 pₜ 的品質上。
2. **流動性風險**：orderbook 深度可能突然消失，Lₜ 會瞬間惡化。
3. **費率變動**：Polymarket 的費率結構可能調整，需定期檢查。
4. **過擬合**：回測時的 r 和模型參數可能過度擬合歷史資料。walk-forward + out-of-sample 是防線。
5. **延遲**：從讀取 orderbook 到下單執行之間有延遲，實際成交價可能與計算不同。
6. **模型校準偏差**：第一版不做校準修正（如 isotonic calibration），第二版再加。
7. **r 設定風險**：reinvestment rate r 若設定不合理（過高或過低），會直接影響進出場判斷的靈敏度。r 需靠回測敏感度分析決定，不可憑感覺設定。
