---
## superpowers / subagent-driven-development

### 來源
- repo：superpowers
- 路徑：skills/subagent-driven-development/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/subagent-driven-development/skill.md

### 一句話定位
在同一會話內用子代理執行獨立實作子任務的開發技能。

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 協作導向, 拆解導向

### 核心思考框架
- [The Process]
- [Example Workflow]

### 核心行為規則
必須做
- - **superpowers:using-git-worktrees** - REQUIRED: Set up isolated workspace before starting
- Implementer: "Before I begin - should the hook be installed at user or system level?"

禁止做
- **Never** ignore an escalation or force the same model to retry without changes. If the implementer said it's stuck, something needs to change.
- - Parallel-safe (subagents don't interfere)
- **Never:**
- - Don't rush them into implementation
- - Don't skip the re-review
- - Don't try to fix manually (context pollution)
- **BLOCKED:** The implementer cannot complete the task. Assess the blocker:

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
- [Prompt Templates]
- - `./implementer-prompt.md` - Dispatch implementer subagent
- - `./spec-reviewer-prompt.md` - Dispatch spec compliance reviewer subagent
- - `./code-quality-reviewer-prompt.md` - Dispatch code quality reviewer subagent

### 適用場景
- 適合在需要「在同一會話內用子代理執行獨立實作子任務的開發技能」的工作階段使用。
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> Use when executing implementation plans with independent tasks in the current session
> When to Use
> **vs. Executing Plans (parallel session):**
> - Same session (no context switch)
> - Fresh subagent per task (no context pollution)
> - Two-stage review after each task: spec compliance first, then code quality
> The Process
> Model Selection
> **Mechanical implementation tasks** (isolated functions, clear specs, 1-2 files): use a fast, cheap model. Most implementation tasks are mechanical when the plan is well-specified.
> **Integration and judgment tasks** (multi-file coordination, pattern matching, debugging): use a standard model.

### 和其他 skill 的潛在關聯
- team（oh-my-claudecode） - 相似 - 共享領域：meta, parallel, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：handling, integration
- project-session-manager（oh-my-claudecode） - 相似 - 共享領域：meta, parallel, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：handling
- claude-api（anthropics-skills） - 相似 - 共享領域：meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：current, prompt, tasks
- omc-teams（oh-my-claudecode） - 相似 - 共享領域：meta, parallel, planning；共享分類：工具程序型、流程型、元技能型

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
