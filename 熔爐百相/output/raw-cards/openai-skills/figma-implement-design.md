---
## openai-skills / figma-implement-design

### 來源
- repo：openai-skills
- 路徑：skills/.curated/figma-implement-design/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/figma-implement-design/skill.md

### 一句話定位
Translates Figma designs into production-ready application code with 1:1 visual…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Overview]
- This skill provides a structured workflow for translating Figma designs into production-ready code with pixel-perfect accuracy. It ensures consistent integration with the Figma MCP server, proper use of design tokens, and 1:1 visual parity with designs.
- [Required Workflow]
- **Follow these steps in order. Do not skip steps.**
- **URL format:** `https://figma.com/design/:fileKey/:fileName?node-id=1-2`
- **Extract:**
- - **File key:** `:fileKey` (the segment after `/design/`)
- - **Node ID:** `1-2` (the value of the `node-id` query parameter)
- **Note:** When using the local desktop MCP (`figma-desktop`), `fileKey` is not passed as a parameter to tool calls. The server automatically uses the currently open file, so only `nodeId` is needed.

### 核心行為規則
必須做
- Before marking complete, validate the final UI against the Figma screenshot.
- - ALWAYS use components from the project's design system when possible

禁止做
- **Follow these steps in order. Do not skip steps.**
- - DO NOT import or add new icon packages - all assets should come from the Figma payload
- - DO NOT use or create placeholders if a `localhost` source is provided
- Never implement based on assumptions. Always fetch `get_design_context` and `get_screenshot` first.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Translates Figma designs into production-ready application code with 1:1 visual…」的工作階段使用。
- 常見觸發語句：implement design / generate code / implement component

### 原文精華摘錄
> Translates Figma designs into production-ready application code with 1:1 visual fidelity
> Use when implementing UI code from Figma files, when user mentions "implement design", "generate code", "implement component", provides Figma URLs, or asks to build components matching Figma specs
> For Figma canvas writes via `use_figma`, use `figma-use`
> Overview
> This skill provides a structured workflow for translating Figma designs into production-ready code with pixel-perfect accuracy. It ensures consistent integration with the Figma MCP server, proper use of design tokens, and 1:1 visual parity with designs.
> Skill Boundaries
> - Use this skill when the deliverable is code in the user's repository.
> - If the user asks to create/edit/delete nodes inside Figma itself, switch to [figma-use](../figma-use/SKILL.md).
> - If the user asks to build or update a full-page screen in Figma from code or a description, switch to [figma-generate-design](../figma-generate-design/SKILL.md).
> - If the user asks only for Code Connect mappings, switch to [figma-code-connect-components](../figma-code-connect-components/SKILL.md).

### 和其他 skill 的潛在關聯
- ui-styling（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：best, common, component, designs
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：build, common, component, examples
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：best, common, files, overview
- paid-ads（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：best, common, mentions, overview

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [x] 人格型
- [x] 流程型
- [x] 元技能型
---
