---
## openai-skills / playwright-interactive

### 來源
- repo：openai-skills
- 路徑：skills/.curated/playwright-interactive/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/playwright-interactive/skill.md

### 一句話定位
"Persistent browser and Electron interaction through js repl for fast iterative…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 證據導向

### 核心思考框架
- [Core Workflow]
- 1. Write a brief QA inventory before testing:
- - Build the inventory from three sources: the user's requested requirements, the user-visible features or behaviors you actually implemented, and the claims you expect to make in the final response.
- - Anything that appears in any of those three sources must map to at least one QA check before signoff.
- - List the user-visible claims you intend to sign off on.
- - List every meaningful user-facing control, mode switch, or implemented interactive behavior.
- - List the state changes or view changes each control or implemented behavior can cause.
- [Checklists]
- - Bootstrap `js_repl` once, then keep the same Playwright handles alive across iterations.
- - Launch the target runtime from the current workspace.
- - Make the code change.
- - Reload or relaunch using the correct path for that change.
- - Update the shared QA inventory if exploration reveals an additional control, state, or visible claim.
- - Re-run functional QA.
- [Common Failure Modes]
- - `Cannot find module 'playwright'`: run the one-time setup in the current workspace and verify the import before using `js_repl`.
- - Playwright package is installed but the browser executable is missing: run `npx playwright install chromium`.
- - `page.goto: net::ERR_CONNECTION_REFUSED`: make sure the dev server is still running in a persistent TTY session, recheck the port, and prefer `http://127.0.0.1:<port>`.

### 核心行為規則
必須做
- Before `page.goto(...)`, verify the chosen port is listening and the app responds.
- Only run cleanup when the task is actually finished:
- - Before signoff, explicitly ask: what visible part of this interface have I not yet inspected closely?
- - Before signoff, explicitly ask: what visible defect would most likely embarrass this result if the user looked closely?
- Do not assume a screenshot is acceptable just because the main widget is visible. Before signoff, explicitly verify that the intended initial view matches the product requirement, using both screenshot review and numeric checks.

禁止做
- - Treat switching modes as a context reset. Do not reuse a viewport-emulated `context` for a native-window pass or vice versa; close the old `page` and `context`, then create a new one for the new mode.
- Do not emit raw native-window screenshots by default. Skip normalization only when you explicitly need device-pixel fidelity, such as Retina or DPI artifact debugging, pixel-accurate rendering inspection, or another fidelity-sensitive case where raw pixels matter more than payload size. For local-only inspection that will not be emitted to the model, raw capture is fine.
- Do not assume `page.screenshot({ scale: "css" })` is enough in native-window mode (`viewport: null`). In Chromium on macOS Retina displays, headed native-window screenshots can still come back at device-pixel size even when `scale: "css"` is requested. The same caveat applies to Electron windows launched through Playwright because Electron runs with `noDefaultViewport`, and `appWindow.screenshot({ scale: "css" })` may still return device-pixel output.
- Do not assume a screenshot is acceptable just because the main widget is visible. Before signoff, explicitly verify that the intended initial view matches the product requirement, using both screenshot review and numeric checks.
- - Do not rely on document scroll metrics alone. Fixed-height shells, internal panes, and hidden-overflow containers can clip required UI while page-level scroll checks still look clean.
- For local web debugging, keep the app running in a persistent TTY session. Do not rely on one-shot background commands from a short-lived shell.

### 提問方式
- - Before signoff, explicitly ask: what visible part of this interface have I not yet inspected closely?
- - Before signoff, explicitly ask: what visible defect would most likely embarrass this result if the user looked closely?

### 審查維度
- [Checklists]
- - Bootstrap `js_repl` once, then keep the same Playwright handles alive across iterations.
- - Launch the target runtime from the current workspace.
- - Make the code change.
- - Reload or relaunch using the correct path for that change.
- - Update the shared QA inventory if exploration reveals an additional control, state, or visible claim.
- - Re-run functional QA.

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Persistent browser and Electron interaction through js repl for fast iterative…」的工作階段使用。
- 常見觸發語句：Persistent browser and Electron interaction through `js_repl` for fast iterative UI debugging.

### 原文精華摘錄
> "Persistent browser and Electron interaction through `js_repl` for fast iterative UI debugging."
> Preconditions
> - `js_repl` must be enabled for this skill.
> - If `js_repl` is missing, enable it in `~/.codex/config.toml`:
> - You can also start a new session with `--enable js_repl` (equivalent to `-c features.js_repl=true`).
> - After enabling `js_repl`, start a new Codex session so the tool list refreshes.
> One-time setup
> If you switch to a different workspace later, repeat setup there.
> Core Workflow
> 1. Write a brief QA inventory before testing:
> - Build the inventory from three sources: the user's requested requirements, the user-visible features or behaviors you actually implemented, and the claims you expect to make in the final response.
> - Anything that appears in any of those three sources must map to at least one QA check before signoff.
> - List the user-visible claims you intend to sign off on.
> Bootstrap (Run Once)
> Binding rules:
> - Use `var` for the shared top-level Playwright handles because later `js_repl` cells reuse them.

### 和其他 skill 的潛在關聯
- webapp-testing（anthropics-skills） - 相似 - 共享領域：browser, debugging, design；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：browser, common, server, web
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：common, examples, interaction, web
- seo-audit（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：審查型、工具程序型、元技能型；共同關鍵詞：common, core, start, web
- programmatic-seo（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：審查型、工具程序型、元技能型；共同關鍵詞：checks, common, core

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
