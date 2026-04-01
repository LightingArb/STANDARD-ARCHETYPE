---
## oh-my-claudecode / omc-teams

### 來源
- repo：oh-my-claudecode
- 路徑：skills/omc-teams/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/omc-teams/skill.md

### 一句話定位
CLI-team runtime for claude, codex, or gemini workers in tmux panes when you ne…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 協作導向

### 核心思考框架
- [Workflow]
- Check tmux explicitly before claiming it is missing:
- - If this fails, report that **tmux is not installed** and stop.
- - If `$TMUX` is set, `omc team` can reuse the current tmux window/panes directly.
- - If `$TMUX` is empty but `CMUX_SURFACE_ID` is set, report that the user is running inside **cmux**. Do **not** say tmux is missing or that they are "not inside tmux"; `omc team` will launch a **detached tmux session** for workers instead of splitting the cmux surface.
- - If neither `$TMUX` nor `CMUX_SURFACE_ID` is set, report that the user is in a **plain terminal**. `omc team` can still launch a **detached tmux session**, but if they specifically want in-place pane/window topology they should start from a classic tmux session first.
- - If you need to confirm the active tmux session, use:

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- Do not claim the team started successfully unless pane output shows the command was submitted.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「CLI-team runtime for claude, codex, or gemini workers in tmux panes when you ne…」的工作階段使用。

### 原文精華摘錄
> CLI-team runtime for claude, codex, or gemini workers in tmux panes when you need process-based parallel execution
> Usage
> - **N** - Number of CLI workers (1-10)
> - **agent-type** - `claude` (Claude CLI), `codex` (OpenAI Codex CLI), or `gemini` (Google Gemini CLI)
> - **task** - Task description to distribute across all workers
> Requirements
> - **tmux binary** must be installed and discoverable (`command -v tmux`)
> - **Classic tmux session optional** for in-place pane splitting (`$TMUX` set). Inside cmux or a plain terminal, `omc team` falls back to a detached tmux session instead of splitting the current surface.
> - **claude** CLI: `npm install -g @anthropic-ai/claude-code`
> - **codex** CLI: `npm install -g @openai/codex`

### 和其他 skill 的潛在關聯
- marketing-ideas（marketingskills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：execution, reference
- product-vision（pm-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：requirements, team
- figma-generate-design（openai-skills） - 相似 - 共享領域：design, meta, parallel；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：error, reference
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：reference

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
