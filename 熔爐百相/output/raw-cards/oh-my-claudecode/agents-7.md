---
## oh-my-claudecode / AGENTS

### 來源
- repo：oh-my-claudecode
- 路徑：src/tools/AGENTS.md
- 檔案類型：AGENTS.md
- card_kind：overview
- language：en
- canonical_group：oh-my-claudecode::src/tools/agents.md

### 一句話定位
IDE-like capabilities for AI agents via Language Server Protocol (LSP), Abstrac…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

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
- [Tool Summary]
- ### LSP Tools (12)
- | Tool | Purpose |
- |------|---------|
- | `lsp_hover` | Type info/docs at position |
- | `lsp_goto_definition` | Jump to symbol definition |
- | `lsp_find_references` | Find all usages |

### 適用場景
- 適合在需要「IDE-like capabilities for AI agents via Language Server Protocol (LSP), Abstrac…」的工作階段使用。

### 原文精華摘錄
> IDE-like capabilities for AI agents via Language Server Protocol (LSP), Abstract Syntax Tree (AST) tools, and Python REPL
> Purpose
> This directory provides agents with powerful code intelligence tools:
> - **LSP Tools (12)**: Hover info, go-to-definition, find references, diagnostics, rename, code actions
> - **AST Tools (2)**: Structural code search and transformation via ast-grep
> - **Python REPL (1)**: Interactive Python execution for data analysis
> Key Files
> | File | Description |
> |------|-------------|
> | `index.ts` | Tool registry - exports `allCustomTools`, `lspTools`, `astTools` |

### 和其他 skill 的潛在關聯
- pdf（openai-skills） - 相似 - 共享領域：meta, shipping；共享分類：審查型、工具程序型、元技能型；共同關鍵詞：dependencies, files, python, tools
- speech（openai-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、工具程序型、元技能型；共同關鍵詞：dependencies, key, tree, via
- mcp-builder（anthropics-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：protocol, python, tools
- skill-creator（anthropics-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：files, key, summary

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [ ] 流程型
- [x] 元技能型
---
