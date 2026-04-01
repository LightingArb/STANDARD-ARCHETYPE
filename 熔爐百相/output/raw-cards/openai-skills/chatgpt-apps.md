---
## openai-skills / chatgpt-apps

### 來源
- repo：openai-skills
- 路徑：skills/.curated/chatgpt-apps/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/chatgpt-apps/skill.md

### 一句話定位
Build, scaffold, refactor, and troubleshoot ChatGPT Apps SDK applications that…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Overview]
- Use this skill to produce:
- - A primary app-archetype classification and repo-shape decision
- - A tool plan (names, schemas, annotations, outputs)
- - An upstream starting-point recommendation (official example, ext-apps example, or local fallback scaffold)
- - An MCP server scaffold (resource registration, tool handlers, metadata)
- - A widget scaffold (MCP Apps bridge first, `window.openai` compatibility/extensions second)
- [Mandatory Docs-First Workflow]
- 1. Invoke `$openai-docs` (preferred) or call the OpenAI docs MCP server directly.
- 2. Fetch current Apps SDK docs before writing code, especially (baseline pages):
- - `apps-sdk/build/mcp-server`
- - `apps-sdk/build/chatgpt-ui`
- - `apps-sdk/build/examples`
- - `apps-sdk/plan/tools`
- [Build Workflow]
- - Prefer a single primary archetype instead of mixing several.
- - If the request is broad, infer the smallest archetype that can still satisfy it.
- - Escalate to `submission-ready` only when the user asks for public launch, directory submission, or review-ready deployment.

### 核心行為規則
必須做
- Before choosing examples, repo shape, or scaffolds, classify the request into one primary archetype and state it.
- ## Classify The App Before Choosing Code
- ### 1. Plan Tools Before Code

禁止做
- Do not generate a large custom scaffold from scratch if a close upstream example already exists.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
- [Output Expectations]
- When using this skill to scaffold code, produce output in this order unless the user asks otherwise:
- - For direct scaffold requests, do not stop at the plan: give the brief plan, then create the files immediately.
- 1. Primary app archetype chosen and why
- 2. Tool plan and architecture choice (minimal vs decoupled)
- 3. Upstream starting point chosen (official example, ext-apps example, or local fallback scaffold) and why
- 4. Doc pages/URLs used from `$openai-docs`

### 適用場景
- 適合在需要「Build, scaffold, refactor, and troubleshoot ChatGPT Apps SDK applications that…」的工作階段使用。

### 原文精華摘錄
> Build, scaffold, refactor, and troubleshoot ChatGPT Apps SDK applications that combine an MCP server and widget UI
> Use when Codex needs to design tools, register UI resources, wire the MCP Apps bridge or ChatGPT compatibility APIs, apply Apps SDK metadata or CSP or domain settings, or produce a docs-aligned project scaffold
> Prefer a docs-first workflow by invoking the openai-docs skill or OpenAI developer docs MCP tools before generating code
> Overview
> Use this skill to produce:
> - A primary app-archetype classification and repo-shape decision
> - A tool plan (names, schemas, annotations, outputs)
> - An upstream starting-point recommendation (official example, ext-apps example, or local fallback scaffold)
> Mandatory Docs-First Workflow
> 1. Invoke `$openai-docs` (preferred) or call the OpenAI docs MCP server directly.

### 和其他 skill 的潛在關聯
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：app, apply, build, mcp
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：apps, build, openai, prompt
- mcp-builder（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：apis, mcp, overview, sdk
- free-tool-strategy（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：build, interactive, overview, tools

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
