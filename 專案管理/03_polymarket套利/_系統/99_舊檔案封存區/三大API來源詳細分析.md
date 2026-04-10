# 三大 API 來源詳細分析

- 分析日期：2026-04-03
- 用途：確認每個來源實際能給我們什麼、有什麼限制
- 範圍：只看溫度相關欄位

---

## 一、IEM（觀測 — 實際發生的溫度）

### 基本資訊

| 項目 | 內容 |
|------|------|
| 全名 | Iowa Environmental Mesonet |
| 用途 | 抓全球機場氣象站的歷史觀測資料 |
| 費用 | 完全免費 |
| 需要 API key | 不用 |
| 資料頻率 | 每 30 分鐘一筆（METAR 觀測） |
| 歷史深度 | 1988 年起（EGLC 站點） |
| 格式 | CSV |

### API 端點

```
https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py
```

### 文件頁面

```
https://mesonet.agron.iastate.edu/request/download.phtml
```

### 溫度相關欄位

| 欄位 | 說明 | 單位 |
|------|------|------|
| `tmpf` | 氣溫 | °F |
| `dwpf` | 露點溫度 | °F |
| `feel` | 體感溫度（風寒/熱指數） | °F |

### 限制

- 溫度單位是華氏，需自己轉攝氏
- 降水資料僅限美國站點，非美國站無降水
- 每次請求上限約 1,000 站年
- 非美國站的資料可能有缺漏

### 結論

**觀測資料的唯一來源，完全夠用。** 免費、穩定、歷史深。

---

## 二、Open-Meteo（預報 — 免費）

### 基本資訊

| 項目 | 內容 |
|------|------|
| 用途 | 歷史預報快照 + 即時預報 |
| 費用 | 非商業免費 |
| 需要 API key | 不用 |
| 資料頻率 | 每小時 |
| 歷史深度 | GFS: 2022 起 / ECMWF: 2024 起 |
| 格式 | JSON |

### API 端點（三個）

| 端點 | 用途 |
|------|------|
| `https://historical-forecast-api.open-meteo.com/v1/forecast` | 歷史預報（有 previous_day） |
| `https://previous-runs-api.open-meteo.com/v1/forecast` | 最近 7 天各次 run 比較 |
| `https://api.open-meteo.com/v1/forecast` | 即時預報 |

### 溫度相關 hourly 欄位

| 欄位 | 說明 | 有 previous_day？ |
|------|------|------------------|
| `temperature_2m` | 地面 2 公尺氣溫 (°C) | 有，day1~day7 |
| `apparent_temperature` | 體感溫度 (°C) | 未測試 |
| `dew_point_2m` | 露點 (°C) | 未測試 |

### 溫度相關 daily 欄位

| 欄位 | 說明 | 有 previous_day？ |
|------|------|------------------|
| `temperature_2m_max` | 日最高溫 (°C) | **不支援** |
| `temperature_2m_min` | 日最低溫 (°C) | **不支援** |

### 支援的氣象模型（35+ 個，溫度相關的主要模型）

| 模型 | 來源 | 解析度 | 歷史深度 |
|------|------|--------|---------|
| `gfs_seamless` | 美國 NOAA | 13km | 2022 起 |
| `ecmwf_ifs025` | 歐洲 ECMWF | 9km | 2024 起 |
| `icon_seamless` | 德國 DWD | 13km | — |
| `ukmo_seamless` | 英國 Met Office | 10km | — |
| `jma_seamless` | 日本 JMA | — | — |
| `best_match` | 自動選最佳 | — | — |

### 限制

- 非商業免費，商業需付費
- daily 端點不支援 `_previous_dayN`（要用 hourly 自己算）
- 日限 10,000 次、月限 300,000 次
- 回傳的 JSON 不含明確的 model run timestamp

### 新發現

- **支援 50+ 個 hourly 變數**，包含土壤溫度、UV 指數、CAPE 等
- **支援 15 分鐘資料**（但僅限歐洲中部和北美）
- **支援 20 個氣壓層的高空資料**
- 以上對我們溫度分析暫時不需要，但未來若做進階模型可能有用

### 結論

**歷史預報的主力來源。** 免費、有 previous_day、支援多模型比對。唯一缺點是 daily 不支援 previous_day，要繞一步從 hourly 算。

---

## 三、Visual Crossing（預報 — 付費，有帳號）

### 基本資訊

| 項目 | 內容 |
|------|------|
| 用途 | 天氣預報 + 歷史觀測 + 歷史預報 |
| 費用 | 有免費額度，進階功能付費 |
| 需要 API key | 要（已有） |
| 資料頻率 | 每小時 / 每日 |
| 格式 | JSON / CSV |

### API 端點

```
https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/[location]/[date1]/[date2]?key=YOUR_KEY
```

### 歷史預報 API 端點（重要！）

```
https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/historicalforecast/[location]/byrun/[start]/[end]
https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/historicalforecast/[location]/bytarget/[start]/[end]
```

### 溫度相關欄位

| 欄位 | 說明 | 單位 |
|------|------|------|
| `temp` | 平均溫度 | 依 unitGroup |
| `tempmax` | 日最高溫 | 依 unitGroup |
| `tempmin` | 日最低溫 | 依 unitGroup |
| `feelslike` | 體感溫度 | 依 unitGroup |
| `feelslikemax` | 體感最高 | 依 unitGroup |
| `feelslikemin` | 體感最低 | 依 unitGroup |
| `dew` | 露點 | 依 unitGroup |

### 歷史預報的兩種查詢模式

| 模式 | 白話 | 範例 |
|------|------|------|
| **byrun** | 「某天的預報看未來好幾天」 | 12/25 發布的預報，看 12/26~12/30 |
| **bytarget** | 「某天被不同時間的預報怎麼看」 | 12/26 被 12/19~12/25 的預報分別怎麼預測 |

### ⚠️ 重大限制

**歷史預報功能需要額外付費訂閱（Historical Forecast Addon）。**

你目前的免費帳號**不能用**歷史預報 API。只能用 Timeline API 查：
- 未來 15 天的即時預報
- 過去日期的歷史觀測值

### 免費帳號能做的

| 功能 | 能用？ |
|------|--------|
| 查未來 15 天預報 | ✅ |
| 查過去某天的觀測值 | ✅ |
| 查過去某天的預報（歷史預報） | ❌ 要付費 |

### 結論

**免費帳號只能當「第二個即時預報來源」，不能查歷史預報。** 但即使如此，作為 Open-Meteo 的交叉驗證仍然有價值。如果未來要做多來源比對，可以考慮付費升級。

---

## 四、三者比較總表

### 觀測（實際溫度）

| | IEM | Visual Crossing |
|---|---|---|
| 有歷史觀測？ | ✅ 1988 起 | ✅ 有，但深度不明 |
| 頻率 | 每 30 分鐘 | 每小時 |
| 費用 | 免費 | 免費額度內 |
| **結論** | **主力** | 備選 |

### 預報（預測溫度）

| | Open-Meteo | Visual Crossing |
|---|---|---|
| 即時預報 | ✅ 免費 | ✅ 免費額度 |
| 歷史預報（過去的預測紀錄） | ✅ 免費，day1~7 | ❌ 要付費 addon |
| 多模型 | ✅ 35+ 個 | ❌ 單一 |
| **結論** | **主力** | 即時補充 |

### 只看溫度，我們實際需要的

| 需求 | 用什麼 |
|------|--------|
| 歷史觀測（回測用） | IEM |
| 歷史預報（回測用） | Open-Meteo |
| 即時觀測（未來上線用） | IEM + Visual Crossing 交叉驗證 |
| 即時預報（未來上線用） | Open-Meteo + Visual Crossing 交叉驗證 |

---

## 五、新發現

1. **Visual Crossing 歷史預報需要付費** — 免費帳號只能查即時預報和歷史觀測，不能查「過去的預測紀錄」
2. **Visual Crossing 也有歷史觀測** — 可以當 IEM 的備選，交叉驗證結算真值
3. **Visual Crossing 有 byrun 和 bytarget 兩種查法** — 如果未來付費，比 Open-Meteo 的 previous_day 更靈活
4. **Open-Meteo 支援 15 分鐘資料** — 歐洲和北美可用，倫敦有覆蓋，比每小時更細
5. **Open-Meteo 有 35+ 個模型** — 不只 GFS，還有 ECMWF、ICON、UKMO 等，未來可做多模型投票

---

## 六、重大發現：19 個模型的歷史預報深度實測（2026-04-03）

對 Open-Meteo 的 30 個模型逐一測試，確認哪些能用於倫敦（EGLC 座標），以及各自的歷史起點：

### 可用模型（19 個，全部免費）

| 歷史起點 | 年數 | 模型名稱 | 來源國家 |
|---------|------|---------|---------|
| 2016-01 | 10 年 | `jma_seamless` | 日本 |
| 2016-01 | 10 年 | `jma_gsm` | 日本 |
| 2017-01 | 9 年 | `knmi_seamless` | 荷蘭 |
| 2017-01 | 9 年 | `dmi_seamless` | 丹麥 |
| 2017-01 | 9 年 | `metno_seamless` | 挪威 |
| 2017-01 | 9 年 | `best_match` | Open-Meteo 自動選 |
| 2021-06 | 5 年 | `gfs_seamless` | 美國 |
| 2021-06 | 5 年 | `gfs_global` | 美國 |
| 2022-06 | 4 年 | `ukmo_seamless` | 英國 |
| 2022-06 | 4 年 | `ukmo_global_deterministic_10km` | 英國 |
| 2023-01 | 3 年 | `icon_seamless` | 德國 |
| 2023-01 | 3 年 | `icon_global` | 德國 |
| 2023-01 | 3 年 | `icon_eu` | 德國（歐洲區域） |
| 2023-01 | 3 年 | `icon_d2` | 德國（高解析度） |
| 2023-01 | 3 年 | `gem_seamless` | 加拿大 |
| 2023-01 | 3 年 | `gem_global` | 加拿大 |
| 2023-01 | 3 年 | `meteofrance_seamless` | 法國 |
| 2024-01 | 2 年 | `meteofrance_arpege_world` | 法國 |
| 2024-01 | 2 年 | `cma_grapes_global` | 中國 |

### 不可用模型（11 個，倫敦座標無資料）

`gfs_hrrr`、`ecmwf_ifs025`、`ecmwf_ifs04`、`ecmwf_aifs025`、`gem_hrdps`、`meteofrance_arome_france`、`knmi_harmonie_arome_europe`、`dmi_harmonie_arome_europe`、`bom_access_global`、`metno_nordic`、`arpae_cosmo_seamless`

（多為區域模型，倫敦不在其覆蓋範圍）

### 關鍵結論

- 歷史預報來源從「1 個模型 4 年」升級為「19 個模型，最長 10 年」
- 全部免費，不需要 Visual Crossing 付費 addon
- 這些都是全球模型，雖然來自不同國家，但都有倫敦的預報資料

---

## 七、核心技術概念：系統性偏差校正

**使用者核心觀點：預報準不準不重要，偏差穩不穩定才重要。**

舉例：
- JMA 每次都比實際高 2°C → 偏差穩定 → 減 2°C 就能用
- 某模型有時高 5°C、有時低 3°C → 偏差亂跳 → 不能用

**穩定的偏差可以校正，不穩定的偏差不能。**

實作方向：
1. 拿每個模型的歷史預報 vs IEM 實際觀測
2. 算偏差的平均值（系統性偏移）和標準差（穩定度）
3. 標準差小 → 校正後納入使用
4. 標準差大 → 丟掉

意義：
- 不追求「最準的模型」，而是找「偏差最穩定的模型」
- 10 年的「穩定偏差」資料 > 4 年的「很準」資料
- 多個校正後的模型還能互相投票，提高信心

---

## 八、對目前計畫的影響

**策略大幅升級：**

- 回測階段：IEM 觀測 + Open-Meteo **多模型**歷史預報 → 全部免費，最長 10 年
- 上線階段：加上 Visual Crossing 即時預報做交叉驗證 → 免費額度夠
- **不需要購買 Visual Crossing 歷史預報 addon**
- 新增任務：對 19 個模型做偏差穩定度分析，篩選出可用模型
