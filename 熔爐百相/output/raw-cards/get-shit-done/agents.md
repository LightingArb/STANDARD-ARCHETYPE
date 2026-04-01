---
## get-shit-done / AGENTS

### 來源
- repo：get-shit-done
- 路徑：docs/AGENTS.md
- 檔案類型：AGENTS.md
- card_kind：overview
- language：en
- canonical_group：get-shit-done::docs/agents.md

### 一句話定位
All 18 specialized agents — roles, tools, spawn patterns, and relationships.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Overview]
- GSD uses a multi-agent architecture where thin orchestrators (workflow files) spawn specialized agents with fresh context windows. Each agent has a focused role, limited tool access, and produces specific artifacts.
- ### Agent Categories
- | Category | Count | Agents |
- |----------|-------|--------|
- | Researchers | 3 | project-researcher, phase-researcher, ui-researcher |
- | Analyzers | 2 | assumptions-analyzer, advisor-researcher |

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Never modifies implementation code — only test files

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
- [Agent Tool Permissions Summary]
- **Principle of Least Privilege:**
- - Checkers are read-only (no Write/Edit) — they evaluate, never modify
- - Researchers have web access — they need current ecosystem information
- - Executors have Edit — they modify code but not web access
- - Mappers have Write — they write analysis documents but not Edit (no code changes)

### 適用場景
- 適合在需要「All 18 specialized agents — roles, tools, spawn patterns, and relationships.」的工作階段使用。
- 常出現在收尾、發布、分支整理或部署交接階段。

### 原文精華摘錄
> All 18 specialized agents — roles, tools, spawn patterns, and relationships
> For architecture context, see [Architecture](ARCHITECTURE.md)
> Overview
> GSD uses a multi-agent architecture where thin orchestrators (workflow files) spawn specialized agents with fresh context windows. Each agent has a focused role, limited tool access, and produces specific artifacts.
> ### Agent Categories
> | Category | Count | Agents |
> |----------|-------|--------|
> Agent Details
> **Role:** Researches domain ecosystem before roadmap creation.
> **Capabilities:**

### 和其他 skill 的潛在關聯
- free-tool-strategy（marketingskills） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：overview, see, tool, tools
- analytics-tracking（marketingskills） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：overview, see, tool
- team（oh-my-claudecode） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：agent, architecture, patterns
- claude-api（anthropics-skills） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：agent, architecture

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
