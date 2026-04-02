# 04_主專案程式

## 這是什麼
這裡放可執行、可交付、可部署的執行單元。

## 建議執行單元

- `001_真值資料收集__探索`
  用來驗證 Polymarket 規則頁與 Wunderground `EGLC` 日歷史頁的真值抓取與 finalized 邏輯。
- `002_預報快照驗證__探索`
  用來驗證 Open-Meteo、Met Office、NOAA 或 ECMWF 的 issue-time forecast 資料可用性。
- `003_誤差統計與訊號研究__子專案`
  用來把 `forecast_error`、`remaining_rise` 與市場 bucket 價格串起來。

## 當前整合方向

- 先把真值、預報快照與統計訊號分成三段，避免資料定義混在一起。
- 在沒確認資料品質前，不建立任何真實執行或自動下單流程。
