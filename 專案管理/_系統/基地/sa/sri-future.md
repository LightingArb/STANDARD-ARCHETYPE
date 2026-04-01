<!-- sri-future.md — SRI 未來議題 -->
<!-- sri_version: 4.0.0 -->

# SRI v4.0.0 — 未來議題

---

## 已從 future 移入 current

以下項目在 v4 已正式納入主系統，不再算 future：

- `01_團隊/` 合併人格與角色
- `03_文件產出/` 作為正式 output 層
- `_系統/基地/mcp/` 作為 MCP 工具層
- 10-15 自動更新機制
- 持續性角色討論機制
- Harness 程式碼修改閘門

---

## 仍在 future 的議題

| 議題 | 為什麼還在 future | 建議歸屬 |
|------|------------------|---------|
| v3→v4 既有專案自動遷移腳本 | 需要先觀察實際專案資料型態 | `sri-core.md` 系統更新子步驟 |
| v4 驗證腳本自動化 | 目前仍以 `rg` / `grep` 人工驗證為主 | `_系統/基地/mcp/` 或外部腳本 |
| MCP server 預設庫 | 需要累積多個穩定 server 樣板 | `_系統/基地/mcp/servers.json` |
| 03_文件產出分類規則細化 | 目前只定義平層優先，尚未定義大型專案分類規則 | `sri-core.md` §7 |
| Harness 進度報告標準化模板 | v4 已有原則，但格式仍可再壓縮 | `sri-ai-templates.md` |
| 系統更新差異報告 | 目前只有 system-log，還沒有結構化 diff 產物 | `sri-ai-templates.md` / `_state/` |

---

## 評估中但尚未立項

- 是否需要 `03_文件產出/` 的版本索引或交付狀態欄位。
- 是否需要為 `_系統/templates/` 增加第三種大型企業專案樣板。
- 是否需要為 `04_主專案程式/` 補通用執行單元 README 產生器。
- 是否需要把 MCP 權限等級擴成 `read-only / read-write / external-write` 三層。

---

## 原則

1. future 只放未定案或未落地項目。
2. 一旦正式寫進 `sri-core-v4.md` 或已落地到衍生物，就從 future 移出。
3. 任何 future 提案若影響路徑、編號、權限邊界，一律視為主版本變更候選。

