---
## openai-skills / notion-spec-to-implementation

### 來源
- repo：openai-skills
- 路徑：skills/.curated/notion-spec-to-implementation/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/notion-spec-to-implementation/skill.md

### 一句話定位
Turn Notion specs into implementation plans, tasks, and progress tracking; use…

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
- 適合在需要「Turn Notion specs into implementation plans, tasks, and progress tracking; use…」的工作階段使用。
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> Turn Notion specs into implementation plans, tasks, and progress tracking
> use when implementing PRDs/feature specs and creating Notion plans + tasks from them
> Quick start
> 1) Locate the spec with `Notion:notion-search`, then fetch it with `Notion:notion-fetch`.
> 2) Parse requirements and ambiguities using `reference/spec-parsing.md`.
> 3) Create a plan page with `Notion:notion-create-pages` (pick a template: quick vs. full).
> 4) Find the task database, confirm schema, then create tasks with `Notion:notion-create-pages`.
> Workflow
> ### 0) If any MCP call fails because Notion MCP is not connected, pause and set it up:
> 1. Add the Notion MCP:

### 和其他 skill 的潛在關聯
- design-system（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、工具程序型、元技能型；共同關鍵詞：quick, references, specs, start
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：quick, tasks
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、流程型、元技能型；共同關鍵詞：creating, quick, references
- README（oh-my-claudecode） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：quick, start

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
