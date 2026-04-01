---
## openai-skills / figma-code-connect-components

### 來源
- repo：openai-skills
- 路徑：skills/.curated/figma-code-connect-components/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/figma-code-connect-components/skill.md

### 一句話定位
Connects Figma design components to code components using Code Connect mapping…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 角色鮮明, 觀點強烈, 規範導向

### 核心思考框架
- [Overview]
- This skill helps you connect Figma design components to their corresponding code implementations using Figma's Code Connect feature. It analyzes the Figma design structure, searches your codebase for matching components, and establishes mappings that maintain design-code consistency.
- [Required Workflow]
- **Follow these steps in order. Do not skip steps.**
- Call `get_code_connect_suggestions` to identify all unmapped components in a single operation. This tool automatically:
- - Fetches component info from the Figma scenegraph
- - Identifies published components in the selection
- - Checks existing Code Connect mappings and filters out already-connected components
- - Returns component names, properties, and thumbnail images for each unmapped component

### 核心行為規則
必須做
- - Ask the user to confirm which component to use or provide the correct path
- **Solution:** Ask the user if the component exists under a different name or in a different location. They may need to create the component first, or it might be located in an unexpected directory.

禁止做
- **Follow these steps in order. Do not skip steps.**
- Don't just ask the user for the file path — actively search their codebase to find matching components. This provides a better experience and catches potential mapping opportunities.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Connects Figma design components to code components using Code Connect mapping…」的工作階段使用。
- 常見觸發語句：code connect / connect this component to code / map this component / link component to code / create code connect mapping
- 常出現在正式施工前的環境準備與工具接線階段。

### 原文精華摘錄
> Connects Figma design components to code components using Code Connect mapping tools
> Use when user says "code connect", "connect this component to code", "map this component", "link component to code", "create code connect mapping", or wants to establish mappings between Figma designs and code impleme...
> For canvas writes via `use_figma`, use `figma-use`
> Overview
> This skill helps you connect Figma design components to their corresponding code implementations using Figma's Code Connect feature. It analyzes the Figma design structure, searches your codebase for matching components, and establishes mappings that mainta...
> Skill Boundaries
> - Use this skill for `get_code_connect_suggestions` + `send_code_connect_mappings` workflows.
> - If the task requires writing to the Figma canvas with Plugin API scripts, switch to [figma-use](../figma-use/SKILL.md).
> - If the task is building or updating a full-page screen in Figma from code or a description, switch to [figma-generate-design](../figma-generate-design/SKILL.md).
> - If the task is implementing product code from Figma, switch to [figma-implement-design](../figma-implement-design/SKILL.md).

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：best, between, common, overview
- ui-styling（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：best, common, component, designs
- paid-ads（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：best, common, overview, practices
- copywriting（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：best, practices, says, wants

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [x] 人格型
- [x] 流程型
- [x] 元技能型
---
