---
## oh-my-claudecode / mcp-setup

### 來源
- repo：oh-my-claudecode
- 路徑：skills/mcp-setup/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/mcp-setup/skill.md

### 一句話定位
Configure popular MCP servers for enhanced agent capabilities

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
- [Overview]
- MCP servers provide additional tools that Claude Code agents can use. This skill helps you configure popular MCP servers using the `claude mcp add` command-line interface.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - If MCP servers don't appear, run `claude mcp list` to check status

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
- [Step 2: Gather Required Information]
- ### For Context7:
- ### For Exa Web Search:
- Ask for API key:
- ### For Filesystem:
- Ask for allowed directories:
- ### For GitHub:

### 適用場景
- 適合在需要「Configure popular MCP servers for enhanced agent capabilities」的工作階段使用。

### 原文精華摘錄
> Configure popular MCP servers for enhanced agent capabilities
> Overview
> MCP servers provide additional tools that Claude Code agents can use. This skill helps you configure popular MCP servers using the `claude mcp add` command-line interface.
> Step 1: Show Available MCP Servers
> Present the user with available MCP server options using AskUserQuestion:
> **Question:** "Which MCP server would you like to configure?"
> **Options:**
> 1. **Context7** - Documentation and code context from popular libraries
> Step 2: Gather Required Information
> ### For Context7:

### 和其他 skill 的潛在關聯
- schema-markup（marketingskills） - 相似 - 共享領域：design, meta, review；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：add, common, enhanced, issues
- figma-create-design-system-rules（openai-skills） - 相似 - 共享領域：design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：common, custom, issues, mcp
- design-html（gstack） - 相似 - 共享領域：design, meta, review；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：completion, server, step
- playwright-interactive（openai-skills） - 相似 - 共享領域：design, meta, review；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：common, required, server

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
