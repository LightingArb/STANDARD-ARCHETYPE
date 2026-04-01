---
## oh-my-claudecode / hud

### 來源
- repo：oh-my-claudecode
- 路徑：skills/hud/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/hud/skill.md

### 一句話定位
Configure HUD display options (layout, presets, display elements)

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- **IMPORTANT**: If the argument is `setup` OR if the HUD script doesn't exist at `~/.claude/hud/omc-hud.mjs`, you MUST create the HUD files directly using the instructions below.
- **IMPORTANT:** The command path MUST use forward slashes on all platforms. Claude Code executes statusLine commands via bash, which interprets backslashes as escape characters and breaks the path.

禁止做
- **IMPORTANT:** Do not use `~` in the command. On Unix, use `$HOME` to keep the path portable across machines. On Windows, use an absolute path because Windows does not expand `~` in shell commands.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Configure HUD display options (layout, presets, display elements)」的工作階段使用。

### 原文精華摘錄
> Configure HUD display options (layout, presets, display elements)
> Quick Commands
> | Command | Description |
> |---------|-------------|
> | `/oh-my-claudecode:hud` | Show current HUD status (auto-setup if needed) |
> | `/oh-my-claudecode:hud setup` | Install/repair HUD statusline |
> Auto-Setup
> When you run `/oh-my-claudecode:hud` or `/oh-my-claudecode:hud setup`, the system will automatically:
> 1. Check if `~/.claude/hud/omc-hud.mjs` exists
> 2. Check if `statusLine` is configured in `~/.claude/settings.json`

### 和其他 skill 的潛在關聯
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：color, elements, layout, quick
- banner-design（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：display, options, quick
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：agent, quick
- docx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：coding, quick

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
