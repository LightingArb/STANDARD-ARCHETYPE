---
## openai-skills / develop-web-game

### 來源
- repo：openai-skills
- 路徑：skills/.curated/develop-web-game/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/develop-web-game/skill.md

### 一句話定位
"Use when Codex is building or iterating on a web game (HTML/JS) and needs a re…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Workflow]
- 1. **Pick a goal.** Define a single feature or behavior to implement.
- 2. **Implement small.** Make the smallest change that moves the game forward.
- 3. **Ensure integration points.** Provide a single canvas and `window.render_game_to_text` so the test loop can read state.
- 4. **Add `window.advanceTime(ms)`.** Strongly prefer a deterministic step hook so the Playwright script can advance frames reliably; without it, automated tests can be flaky.
- 5. **Initialize progress.md.** If `progress.md` exists, read it first and confirm the original user prompt is recorded at the top (prefix with `Original prompt:`). Also note any TODOs and suggestions left by the previous agent. If missing, create it and write `Original prompt: <prompt>` at the top before appending updates.
- 6. **Verify Playwright availability.** Ensure `playwright` is available (local dependency or global install). If unsure, check `npx` first.
- [Test Checklist]
- Examples of things to test:
- - Primary movement/interaction inputs (e.g., move, jump, shoot, confirm/select).
- - Win/lose or success/fail transitions.
- - Score/health/resource changes.
- - Boundary conditions (collisions, walls, screen edges).
- - Menu/pause/start flow if present.
- [Core Game Guidelines]
- - Prefer a single canvas centered in the window.
- - Keep on-screen text minimal; show controls on a start/menu screen rather than overlaying them during play.
- - Avoid overly dark scenes unless the design calls for it. Make key elements easy to see.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- If a `progress.md` file already exists, read it first, including the original user prompt at the top (you may be continuing another agent's work). Do not overwrite the original prompt; preserve it.
- - Do not switch to `@playwright/test` unless explicitly asked; stick to the client script.

### 提問方式
無明確提問模板

### 審查維度
- [Test Checklist]
- Examples of things to test:
- - Primary movement/interaction inputs (e.g., move, jump, shoot, confirm/select).
- - Win/lose or success/fail transitions.
- - Score/health/resource changes.
- - Boundary conditions (collisions, walls, screen edges).
- - Menu/pause/start flow if present.
- [Test Artifacts to Review]
- - Latest screenshots from the Playwright run.
- - Latest `render_game_to_text` JSON output.
- - Console error logs (fix the first new error before continuing).

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use when Codex is building or iterating on a web game (HTML/JS) and needs a re…」的工作階段使用。
- 常見觸發語句：Use when Codex is building or iterating on a web game (HTML/JS) and needs a reliable development + testing loop: implement small changes, run a Playwright-based test script with short input bursts and intentional pauses, inspect screenshots/text, and review console errors with render_game_to_text.
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> "Use when Codex is building or iterating on a web game (HTML/JS) and needs a reliable development + testing loop: implement small changes, run a Playwright-based test script with short input bursts and intentional pau...
> Skill paths (set once)
> User-scoped skills install under `$CODEX_HOME/skills` (default: `~/.codex/skills`).
> Workflow
> 1. **Pick a goal.** Define a single feature or behavior to implement.
> 2. **Implement small.** Make the smallest change that moves the game forward.
> 3. **Ensure integration points.** Provide a single canvas and `window.render_game_to_text` so the test loop can read state.
> 4. **Add `window.advanceTime(ms)`.** Strongly prefer a deterministic step hook so the Playwright script can advance frames reliably; without it, automated tests can be flaky.
> Test Checklist
> Examples of things to test:

### 和其他 skill 的潛在關聯
- webapp-testing（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：playwright, screenshots, testing, web
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：checklist, guidelines, html, implement
- design-html（gstack） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：html, implement, input, loop
- schema-markup（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：core, testing

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
