---
## openai-skills / notion-knowledge-capture

### 來源
- repo：openai-skills
- 路徑：skills/.curated/notion-knowledge-capture/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/notion-knowledge-capture/skill.md

### 一句話定位
Capture conversations and decisions into structured Notion pages; use when turn…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Workflow]
- ### 0) If any MCP call fails because Notion MCP is not connected, pause and set it up:
- 1. Add the Notion MCP:
- - `codex mcp add notion --url https://mcp.notion.com/mcp`
- 2. Enable remote MCP client:
- - Set `[features].rmcp_client = true` in `config.toml` **or** run `codex --enable rmcp_client`
- 3. Log in with OAuth:

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Capture conversations and decisions into structured Notion pages; use when turn…」的工作階段使用。

### 原文精華摘錄
> Capture conversations and decisions into structured Notion pages
> use when turning chats/notes into wiki entries, how-tos, decisions, or FAQs with proper linking
> Quick start
> 1) Clarify what to capture (decision, how-to, FAQ, learning, documentation) and target audience.
> 2) Identify the right database/template in `reference/` (team wiki, how-to, FAQ, decision log, learning, documentation).
> 3) Pull any prior context from Notion with `Notion:notion-search` → `Notion:notion-fetch` (existing pages to update/link).
> 4) Draft the page with `Notion:notion-create-pages` using the database’s schema; include summary, context, source links, and tags/owners.
> Workflow
> ### 0) If any MCP call fails because Notion MCP is not connected, pause and set it up:
> 1. Add the Notion MCP:

### 和其他 skill 的潛在關聯
- seo-audit（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：pages, references, start, structured
- schema-markup（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：pages, quick, structured
- pdf（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：pages, quick, start
- lead-magnets（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：capture, notion

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
