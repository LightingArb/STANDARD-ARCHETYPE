---
## openai-skills / playwright

### 來源
- repo：openai-skills
- 路徑：skills/.curated/playwright/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/playwright/skill.md

### 一句話定位
"Use when the task requires automating a real browser from the terminal (naviga…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 證據導向

### 核心思考框架
- [Core workflow]
- 1. Open the page.
- 2. Snapshot to get stable element refs.
- 3. Interact using refs from the latest snapshot.
- 4. Re-snapshot after navigation or significant DOM changes.
- 5. Capture artifacts (screenshot, pdf, traces) when useful.
- Minimal loop:
- [Recommended patterns]
- ### Form fill and submit
- ### Debug a UI flow with traces
- ### Multi-tab work

### 核心行為規則
必須做
- Before proposing commands, check whether `npx` is available (the wrapper depends on it):

禁止做
- Treat this skill as CLI-first automation. Do not pivot to `@playwright/test` unless the user explicitly asks for test files.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use when the task requires automating a real browser from the terminal (naviga…」的工作階段使用。
- 常見觸發語句：Use when the task requires automating a real browser from the terminal (navigation, form filling, snapshots, screenshots, data extraction, UI-flow debugging) via `playwright-cli` or the bundled wrapper script.

### 原文精華摘錄
> "Use when the task requires automating a real browser from the terminal (navigation, form filling, snapshots, screenshots, data extraction, UI-flow debugging) via `playwright-cli` or the bundled wrapper script."
> Prerequisite check (required)
> Before proposing commands, check whether `npx` is available (the wrapper depends on it):
> If it is not available, pause and ask the user to install Node.js/npm (which provides `npx`). Provide these steps verbatim:
> Skill path (set once)
> User-scoped skills install under `$CODEX_HOME/skills` (default: `~/.codex/skills`).
> Quick start
> Use the wrapper script:
> If the user prefers a global install, this is also valid:
> Core workflow

### 和其他 skill 的潛在關聯
- webapp-testing（anthropics-skills） - 相似 - 共享領域：browser, debugging, design；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：browser, playwright, screenshots
- ui-styling（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：core, navigation, patterns, quick
- ab-test-setup（marketingskills） - 相似 - 共享領域：browser, debugging, design；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：core
- seo-audit（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、元技能型；共同關鍵詞：core, data, references, start

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
