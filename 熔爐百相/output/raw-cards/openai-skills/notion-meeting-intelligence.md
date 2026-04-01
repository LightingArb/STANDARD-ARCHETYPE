---
## openai-skills / notion-meeting-intelligence

### 來源
- repo：openai-skills
- 路徑：skills/.curated/notion-meeting-intelligence/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/notion-meeting-intelligence/skill.md

### 一句話定位
Prepare meeting materials with Notion context and Codex research; use when gath…

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
- 適合在需要「Prepare meeting materials with Notion context and Codex research; use when gath…」的工作階段使用。

### 原文精華摘錄
> Prepare meeting materials with Notion context and Codex research
> use when gathering context, drafting agendas/pre-reads, and tailoring materials to attendees
> Quick start
> 1) Confirm meeting goal, attendees, date/time, and decisions needed.
> 2) Gather context: search with `Notion:notion-search`, then fetch with `Notion:notion-fetch` (prior notes, specs, OKRs, decisions).
> 3) Pick the right template via `reference/template-selection-guide.md` (status, decision, planning, retro, 1:1, brainstorming).
> 4) Draft agenda/pre-read in Notion with `Notion:notion-create-pages`, embedding source links and owner/timeboxes.
> Workflow
> ### 0) If any MCP call fails because Notion MCP is not connected, pause and set it up:
> 1. Add the Notion MCP:

### 和其他 skill 的潛在關聯
- doc-coauthoring（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：context, drafting, gathering
- customer-research（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：gathering, research
- marketing-psychology（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：context, quick
- README（oh-my-claudecode） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：quick, start

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
