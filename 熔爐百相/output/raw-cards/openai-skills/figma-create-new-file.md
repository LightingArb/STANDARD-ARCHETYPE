---
## openai-skills / figma-create-new-file

### 來源
- repo：openai-skills
- 路徑：skills/.curated/figma-create-new-file/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/figma-create-new-file/skill.md

### 一句話定位
Create a new blank Figma file.

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 品味導向, 體驗敏感

### 核心思考框架
- [Workflow]
- The `create_new_file` tool requires a `planKey` parameter. Follow this decision tree:
- 1. **User already provided a planKey** (e.g. from a previous `whoami` call or in their prompt) → use it directly, skip to Step 2.
- 2. **No planKey available** → call the `whoami` tool. The response contains a `plans` array. Each plan has a `key`, `name`, `seat`, and `tier`.
- - **Single plan**: use its `key` field automatically.
- - **Multiple plans**: ask the user which team or organization they want to create the file in, then use the corresponding plan's `key`.
- Call the `create_new_file` tool with:

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
- 適合在需要「Create a new blank Figma file.」的工作階段使用。
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> Create a new blank Figma file
> Use when the user wants to create a new Figma design or FigJam file, or when you need a new file before calling use_figma
> Handles plan resolution via whoami if needed
> Usage — /figma-create-new-file [editorType] [fileName] (e.g
> /figma-create-new-file figjam My Whiteboard)
> Skill Arguments
> - **editorType**: `design` (default) or `figjam`
> - **fileName**: Name for the new file (defaults to "Untitled")
> Examples:
> - `/figma-create-new-file` — creates a design file named "Untitled"

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：file, important, new, wants
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：流程型、元技能型；共同關鍵詞：file, filename, notes, plan
- site-architecture（marketingskills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：need, plan, wants
- office-hours（gstack） - 相似 - 共享領域：design, planning；共同關鍵詞：important, new, plan, wants

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
