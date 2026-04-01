---
## openai-skills / linear

### 來源
- repo：openai-skills
- 路徑：skills/.curated/linear/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/linear/skill.md

### 一句話定位
Manage issues, projects & team workflows in Linear.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Overview]
- This skill provides a structured workflow for managing issues, projects & team workflows in Linear. It ensures consistent integration with the Linear MCP server, which offers natural-language project management for issues, projects, documentation, and team collaboration.
- [Required Workflow]
- **Follow these steps in order. Do not skip steps.**
- If any MCP call fails because Linear MCP is not connected, pause and set it up:
- 1. Add the Linear MCP:
- - `codex mcp add linear --url https://mcp.linear.app/mcp`
- 2. Enable remote MCP client:
- - Set `[features] rmcp_client = true` in `config.toml` **or** run `codex --enable rmcp_client`
- [Practical Workflows]
- - Sprint Planning: Review open issues for a target team, pick top items by priority, and create a new cycle (e.g., "Q1 Performance Sprint") with assignments.
- - Bug Triage: List critical/high-priority bugs, rank by user impact, and move the top items to "In Progress."
- - Documentation Audit: Search documentation (e.g., API auth), then open labeled "documentation" issues for gaps or outdated sections with detailed fixes.
- - Team Workload Balance: Group active issues by assignee, flag anyone with high load, and suggest or apply redistributions.
- - Release Planning: Create a project (e.g., "v2.0 Release") with milestones (feature freeze, beta, docs, launch) and generate issues with estimates.
- - Cross-Project Dependencies: Find all "blocked" issues, identify blockers, and create linked issues if missing.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- **Follow these steps in order. Do not skip steps.**
- - Cross-Project Dependencies: Find all "blocked" issues, identify blockers, and create linked issues if missing.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Manage issues, projects & team workflows in Linear.」的工作階段使用。

### 原文精華摘錄
> Manage issues, projects & team workflows in Linear
> Use when the user wants to read, create or updates tickets in Linear
> Overview
> This skill provides a structured workflow for managing issues, projects & team workflows in Linear. It ensures consistent integration with the Linear MCP server, which offers natural-language project management for issues, projects, documentation, and team...
> Prerequisites
> - Linear MCP server must be connected and accessible via OAuth
> - Confirm access to the relevant Linear workspace, teams, and projects
> Required Workflow
> **Follow these steps in order. Do not skip steps.**
> If any MCP call fails because Linear MCP is not connected, pause and set it up:

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：overview, read, wants, workflows
- free-tool-strategy（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：overview, tools, wants
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：prerequisites, projects, tips
- docx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：overview, read, wants

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
