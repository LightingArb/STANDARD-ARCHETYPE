---
## openai-skills / figma

### 來源
- repo：openai-skills
- 路徑：skills/.curated/figma/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/figma/skill.md

### 一句話定位
Use the Figma MCP server to fetch design context, screenshots, variables, and a…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - IMPORTANT: DO NOT import/add new icon packages, all the assets should be in the Figma payload.
- - The client cannot browse the URL but extracts the node ID from the link; always ensure the link points to the exact node/variant you want.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Use the Figma MCP server to fetch design context, screenshots, variables, and a…」的工作階段使用。
- 常出現在正式施工前的環境準備與工具接線階段。

### 原文精華摘錄
> Use the Figma MCP server to fetch design context, screenshots, variables, and assets from Figma, and to translate Figma nodes into production code
> Trigger when a task involves Figma URLs, node IDs, design-to-code implementation, or Figma MCP setup and troubleshooting
> Figma MCP Integration Rules
> 1. Run get_design_context first to fetch the structured representation for the exact node(s).
> 2. If the response is too large or truncated, run get_metadata to get the high-level node map and then re-fetch only the required node(s) with get_design_context.
> 3. Run get_screenshot for a visual reference of the node variant being implemented.
> 4. Only after you have both get_design_context and get_screenshot, download any assets needed and start implementation.
> References
> - `references/figma-mcp-config.md` — setup, verification, troubleshooting, and link-based usage reminders.
> - `references/figma-tools-and-prompts.md` — tool catalog and prompt patterns for selecting frameworks/components and fetching metadata.

### 和其他 skill 的潛在關聯
- design-html（gstack） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：rules, server, setup
- design（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：integration, references, setup
- popup-cro（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：rules, trigger
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：mcp, rules

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [ ] 流程型
- [x] 元技能型
---
