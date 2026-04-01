---
## openai-skills / openai-docs

### 來源
- repo：openai-skills
- 路徑：skills/.system/openai-docs/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.system/openai-docs/skill.md

### 一句話定位
"Use when the user asks how to build with OpenAI products or APIs and needs up-…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 證據導向

### 核心思考框架
- [Workflow]
- 1. Clarify the product scope and whether the request is general docs lookup, model selection, a GPT-5.4 upgrade, or a GPT-5.4 prompt upgrade.
- 2. If it is a model-selection request, load `references/latest-model.md`.
- 3. If it is an explicit GPT-5.4 upgrade request, load `references/upgrading-to-gpt-5p4.md`.
- 4. If the upgrade may require prompt changes, or the workflow is research-heavy, tool-heavy, coding-oriented, multi-agent, or long-running, also load `references/gpt-5p4-prompting-guide.md`.
- 5. Search docs with a precise query.
- 6. Fetch the best page and the exact section needed (use `anchor` when possible).

### 核心行為規則
必須做
- 4. Ask the user to restart Codex.

禁止做
- 2. If it fails due to permissions/sandboxing, immediately retry the same command with escalated permissions and include a 1-sentence justification for approval. Do not ask the user to run it yet.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use when the user asks how to build with OpenAI products or APIs and needs up-…」的工作階段使用。
- 常見觸發語句：Use when the user asks how to build with OpenAI products or APIs and needs up-to-date official documentation with citations, help choosing the latest model for a use case, or explicit GPT-5.4 upgrade and prompt-upgrade guidance; prioritize OpenAI docs MCP tools, use bundled references only as helper context, and restrict any fallback browsing to official OpenAI domains.

### 原文精華摘錄
> "Use when the user asks how to build with OpenAI products or APIs and needs up-to-date official documentation with citations, help choosing the latest model for a use case, or explicit GPT-5.4 upgrade and prompt-upgra...
> prioritize OpenAI docs MCP tools, use bundled references only as helper context, and restrict any fallback browsing to official OpenAI domains."
> Quick start
> - Use `mcp__openaiDeveloperDocs__search_openai_docs` to find the most relevant doc pages.
> - Use `mcp__openaiDeveloperDocs__fetch_openai_doc` to pull exact sections and quote/paraphrase accurately.
> - Use `mcp__openaiDeveloperDocs__list_openai_docs` only when you need to browse or discover pages without a clear query.
> - Load only the relevant file from `references/` when the question is about model selection or a GPT-5.4 upgrade.
> OpenAI product snapshots
> 1. Apps SDK: Build ChatGPT apps by providing a web component UI and an MCP server that exposes your app's tools to ChatGPT.
> 2. Responses API: A unified endpoint designed for stateful, multimodal, tool-using interactions in agentic workflows.

### 和其他 skill 的潛在關聯
- doc-coauthoring（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：context, docs, documentation, guidance
- mcp-builder（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：apis, context, documentation, mcp
- marketing-ideas（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：asks, case, needs, product
- schema-markup（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：product, quick, reference

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
