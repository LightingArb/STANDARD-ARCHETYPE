---
## anthropics-skills / webapp-testing

### 來源
- repo：anthropics-skills
- 路徑：skills/webapp-testing/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：anthropics-skills::skills/webapp-testing/skill.md

### 一句話定位
Toolkit for interacting with and testing local web applications using Playwrigh…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 證據導向

### 核心思考框架
- [Decision Tree: Choosing Your Approach]
- [Reconnaissance-Then-Action Pattern]
- 1. **Inspect rendered DOM**:
- 2. **Identify selectors** from inspection results
- 3. **Execute actions** using discovered selectors

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- **Always run scripts with `--help` first** to see usage. DO NOT read the source until you try running the script first and find that a customized solution is abslutely necessary. These scripts can be very large and thus pollute your context window. They exist to be called directly as black-box scripts rather than ingested into your context window.
- ❌ **Don't** inspect the DOM before waiting for `networkidle` on dynamic apps

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Toolkit for interacting with and testing local web applications using Playwrigh…」的工作階段使用。

### 原文精華摘錄
> Toolkit for interacting with and testing local web applications using Playwright
> Supports verifying frontend functionality, debugging UI behavior, capturing browser screenshots, and viewing browser logs
> Decision Tree: Choosing Your Approach
> Example: Using with_server.py
> To start a server, run `--help` first, then use the helper:
> **Single server:**
> **Multiple servers (e.g., backend + frontend):**
> To create an automation script, include only Playwright logic (servers are managed automatically):
> Reconnaissance-Then-Action Pattern
> 1. **Inspect rendered DOM**:

### 和其他 skill 的潛在關聯
- develop-web-game（openai-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：playwright, screenshots, testing, web
- aspnet-core（openai-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、流程型、元技能型；共同關鍵詞：applications, reference, testing, web
- playwright-interactive（openai-skills） - 相似 - 共享領域：browser, debugging, design；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：browser, common, server, web
- schema-markup（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：common, reference, testing

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
