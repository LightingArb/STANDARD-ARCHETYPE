---
## oh-my-claudecode / team

### 來源
- repo：oh-my-claudecode
- 路徑：skills/team/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/team/skill.md

### 一句話定位
N coordinated agents on shared task list using Claude Code native teams

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Workflow]
- - Extract **N** (agent count), validate 1-20
- - Extract **agent-type**, validate it maps to a known OMC subagent
- - Extract **task** description
- Use `explore` or `architect` (via MCP or agent) to analyze the codebase and break the task into N subtasks:
- - Each subtask should be **file-scoped** or **module-scoped** to avoid conflicts
- - Subtasks must be independent or have clear dependency ordering
- [Communication Patterns]
- **CRITICAL: Steps must execute in exact order. Never call TeamDelete before shutdown is confirmed.**
- **Step 1: Verify completion**
- **Step 2: Request shutdown from each teammate**
- **Lead sends:**
- **Step 3: Wait for responses (BLOCKING)**
- - Wait up to 30s per teammate for `shutdown_response`

### 核心行為規則
必須做
- **Each completing stage MUST produce a handoff document before transitioning.**
- - ALWAYS use absolute file paths
- - ALWAYS report progress via SendMessage to "team-lead"
- 1. **Worktree creation**: Before spawning a worker, call `createWorkerWorktree(teamName, workerName, repoRoot)` to create an isolated worktree at `.omc/worktrees/{team}/{worker}` with branch `omc-team/{teamName}/{workerName}`.

禁止做
- 2. **Specialist agents complement executor agents.** Route analysis/review to architect/critic Claude agents and UI work to designer agents. Tmux CLI workers are one-shot and don't participate in team communication.
- - NEVER spawn sub-agents or use the Task tool
- - NEVER run tmux pane/session orchestration commands (for example `tmux split-window`, `tmux new-session`)
- - NEVER run team spawning/orchestration skills or commands (for example `$team`, `$ultrawork`, `$autopilot`, `$ralph`, `omc team ...`, `omx team ...`)
- **CRITICAL: Steps must execute in exact order. Never call TeamDelete before shutdown is confirmed.**
- **Shutdown sequence is BLOCKING:** Do not proceed to TeamDelete until all teammates have either:
- 6. **Teammate prompt stored in config** -- The full prompt text is stored in `config.json` members array. Do not put secrets or sensitive data in teammate prompts.
- 7. **Members auto-removed on shutdown** -- After a teammate approves shutdown and terminates, it is automatically removed from `config.json`. Do not re-read config expecting to find shut-down teammates.
- 11. **CLI workers are one-shot, not persistent** -- Tmux CLI workers have full filesystem access and CAN make code changes. However, they run as autonomous one-shot jobs -- they cannot use TaskList/TaskUpdate/SendMessage. The lead must manage their lifecycle: write prompt_file, spawn CLI worker, read output_file, mark task complete. They don't participate in team communication like Claude teammates do.
- 2. work reaches an explicit terminal blocked/failed outcome with evidence.
- "blockedBy": []
- If a task has blockedBy dependencies, skip it until those tasks are completed.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「N coordinated agents on shared task list using Claude Code native teams」的工作階段使用。

### 原文精華摘錄
> N coordinated agents on shared task list using Claude Code native teams
> Usage
> - **N** - Number of teammate agents (1-20). Optional; defaults to auto-sizing based on task decomposition.
> - **agent-type** - OMC agent to spawn for the `team-exec` stage (e.g., executor, debugger, designer, codex, gemini). Optional; defaults to stage-aware routing. Use `codex` to spawn Codex CLI workers or `gemini` for Gemini CLI workers (requires respective CL...
> - **task** - High-level task to decompose and distribute among teammates
> - **ralph** - Optional modifier. When present, wraps the team pipeline in Ralph's persistence loop (retry on failure, architect verification before completion). See Team + Ralph Composition below.
> Architecture
> **Storage layout (managed by Claude Code):**
> Staged Pipeline (Canonical Team Runtime)
> Team execution follows a staged pipeline:
> **Routing rules:**
> 1. **The lead picks agents per stage, not the user.** The user's `N:agent-type` parameter only overrides the `team-exec` stage worker type. All other stages use stage-appropriate specialists.
> 2. **Specialist agents complement executor agents.** Route analysis/review to architect/critic Claude agents and UI work to designer agents. Tmux CLI workers are one-shot and don't participate in team communication.
> Handoff: <current-stage> → <next-stage>
> - **Decided**: [key decisions made in this stage]
> - **Rejected**: [alternatives considered and why they were rejected]

### 和其他 skill 的潛在關聯
- Task Management & Collaboration（awesome-startup） - 相似 - 共享領域：planning, shipping；共享分類：工具程序型；共同關鍵詞：communication, integration, list, task
- design-html（gstack） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：dynamic, patterns, preamble
- xlsx（anthropics-skills） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：integration, pipeline, task
- AGENTS（get-shit-done） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：agent, architecture, patterns

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [x] 人格型
- [x] 流程型
- [x] 元技能型
---
