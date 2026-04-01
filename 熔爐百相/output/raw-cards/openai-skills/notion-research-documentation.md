---
## openai-skills / notion-research-documentation

### 來源
- repo：openai-skills
- 路徑：skills/.curated/notion-research-documentation/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/notion-research-documentation/skill.md

### 一句話定位
Research across Notion and synthesize into structured documentation; use when g…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

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
- 適合在需要「Research across Notion and synthesize into structured documentation; use when g…」的工作階段使用。

### 原文精華摘錄
> Research across Notion and synthesize into structured documentation
> use when gathering info from multiple Notion sources to produce briefs, comparisons, or reports with citations
> Quick start
> 1) Find sources with `Notion:notion-search` using targeted queries; confirm scope with the user.
> 2) Fetch pages via `Notion:notion-fetch`; note key sections and capture citations (`reference/citations.md`).
> 3) Choose output format (brief, summary, comparison, comprehensive report) using `reference/format-selection-guide.md`.
> 4) Draft in Notion with `Notion:notion-create-pages` using the matching template (quick, summary, comparison, comprehensive).
> Workflow
> ### 0) If any MCP call fails because Notion MCP is not connected, pause and set it up:
> 1. Add the Notion MCP:

### 和其他 skill 的潛在關聯
- customer-research（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：gathering, research, sources, synthesize
- pdf（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：multiple, produce, quick, start
- doc-coauthoring（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：documentation, gathering, structured
- schema-markup（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：multiple, quick, structured

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
