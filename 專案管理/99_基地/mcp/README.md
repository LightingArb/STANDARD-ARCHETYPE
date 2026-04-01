# MCP 工具層

這裡放 SRI 的 MCP 設定與安全政策。

## 檔案

- `README.md`：本檔
- `servers.json`：可用 MCP server 清單
- `policies.md`：安全政策

## 原則

1. 所有 MCP server 預設為 read-only。
2. 首次連線必須使用者確認。
3. 會寫入外部系統的 server 永遠需要使用者確認。
4. MCP 產物路由固定到 `02_參考資料/`、`03_文件產出/`、`04_主專案程式/`。
