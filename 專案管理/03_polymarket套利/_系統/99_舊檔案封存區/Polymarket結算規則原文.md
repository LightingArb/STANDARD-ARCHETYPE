# Polymarket 結算規則原文（從 description 欄位提取）

- 提取日期：2026-04-03
- 來源：scan_raw_markets.json 的 description 欄位
- 已驗證：4 種類型各取 1 個樣本

---

## 類型 A：WU + °C（London 為例）

**結算來源：** Wunderground，London City Airport Station (EGLC)
**結算網址：** https://www.wunderground.com/history/daily/gb/london/EGLC
**精度：** 整數攝氏（whole degrees Celsius, eg 9°C）
**結算值：** "the highest temperature recorded for all times on this day"
**finalized 規則：** "This market can not resolve to 'Yes' until all data for this date has been finalized."
**事後修正：** "Any revisions to temperatures recorded after data is finalized for this market's timeframe will not be considered."
**endDate：** 2026-04-02T12:00:00Z（UTC 中午）

---

## 類型 B：WU + °F（Dallas 為例）

**結算來源：** Wunderground，Dallas Love Field Station (**KDAL**，不是 KDFW!)
**結算網址：** https://www.wunderground.com/history/daily/us/tx/dallas/KDAL
**精度：** 整數華氏（whole degrees Fahrenheit, eg 21°F）
**其他規則：** 與類型 A 完全相同，只差單位
**特殊說明：** "To toggle between Fahrenheit and Celsius, click the gear icon"

**重要：** °F 市場的 bucket 是 range 格式（72-73°F），不是 exact

---

## 類型 C：NOAA（Istanbul 為例）

**結算來源：** NOAA，Istanbul Airport (LTFM)
**結算網址：** https://www.weather.gov/wrh/timeseries?site=LTFM
**精度：** 整數攝氏（whole degrees Celsius, eg 9°C）
**結算值：** "the highest reading under the 'Temp' column on the specified date"
**特殊說明：** "click 'Switch to Metric Units' button"
**其他規則：** 與 WU 類相同（finalized、事後修正不算）

**注意：** resolutionSource 欄位為空，結算網址只在 description 中

**⚠️ 已驗證：weather.gov/wrh/timeseries 是美國西部專用工具，不支援國際站。** 實際打開 LTFM 的 URL 顯示的是美國西部站列表，不是 Istanbul 的資料。**Polymarket 的 description 中寫的結算 URL 可能是錯的或是通用格式。需要找到 NOAA 查國際站資料的正確方式。**

---

## 類型 D：香港天文台（Hong Kong）

**結算來源：** Hong Kong Observatory
**結算網址：** https://www.weather.gov.hk/en/cis/climat.htm
**精度：** **攝氏 1 位小數**（temperatures in Celsius to one decimal place, eg 9.1°C）
**結算值：** "Absolute Daily Max (deg. C)" in "Daily Extract"
**其他規則：** 與其他類相同

**這是唯一精度不是整數的城市！** Bucket 定義會完全不同。

---

## 關鍵規則摘要

| 項目 | 內容 |
|------|------|
| 結算值 | 當天所有時間的最高溫 |
| finalized | 必須等 data finalized 後才結算 |
| 事後修正 | finalized 後的修正不算 |
| endDate | UTC 12:00（但不等於結算完成時間） |
| WU 精度 | °C 市場=整數攝氏 / °F 市場=整數華氏 |
| NOAA 精度 | 整數攝氏 |
| HKO 精度 | **1 位小數攝氏** |

## 未解決問題

- "finalized" 具體是什麼時候？WU 頁面會標記嗎？
- endDate UTC 12:00 是市場關閉時間，結算在什麼時候完成？
- WU 的「整數」是四捨五入、無條件捨去、還是直接截斷小數？
- NOAA timeseries URL 能查國際站嗎？（需要實際打開確認）
