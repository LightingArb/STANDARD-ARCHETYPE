---
## openai-skills / screenshot

### 來源
- repo：openai-skills
- 路徑：skills/.curated/screenshot/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/screenshot/skill.md

### 一句話定位
"Use when the user explicitly asks for a desktop or system screenshot (full scr…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Use this skill when explicitly asked, for whole-system desktop captures, or when a tool-specific capture cannot get what you need.
- Use these when you cannot run the helpers.
- - If you see "screen capture checks are blocked in the sandbox", "could not create image from display", or Swift `ModuleCache` permission errors in a sandboxed run, rerun the command with escalated permissions.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use when the user explicitly asks for a desktop or system screenshot (full scr…」的工作階段使用。
- 常見觸發語句：Use when the user explicitly asks for a desktop or system screenshot (full screen, specific app or window, or a pixel region), or when tool-specific capture capabilities are unavailable and an OS-level capture is needed.

### 原文精華摘錄
> "Use when the user explicitly asks for a desktop or system screenshot (full screen, specific app or window, or a pixel region), or when tool-specific capture capabilities are unavailable and an OS-level capture is nee...
> Tool priority
> - Prefer tool-specific screenshot capabilities when available (for example: a Figma MCP/skill for Figma files, or Playwright/agent-browser tools for browsers and Electron apps).
> - Use this skill when explicitly asked, for whole-system desktop captures, or when a tool-specific capture cannot get what you need.
> - Otherwise, treat this skill as the default for desktop apps without a better-integrated capture tool.
> macOS permission preflight (reduce repeated prompts)
> command when possible:
> For Codex inspection runs, keep the output in temp:
> macOS and Linux (Python helper)
> Run the helper from the repo root:

### 和其他 skill 的潛在關聯
- ad-creative（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：full, tool
- design（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：screenshot, system
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：app, priority
- pdf（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：asks, python

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
