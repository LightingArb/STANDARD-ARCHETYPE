# 002_API服務__子專案

## 目標

- 提供行情、帳戶、策略控制與風控 API。

## 結構

- `src/`：FastAPI 服務、adapter、策略與風控模組。
- `output/`：壓測輸出、mock 回應與 sample log。

## 目前決策

- 對外契約先以 REST 為主，內部事件流留給第二階段。
