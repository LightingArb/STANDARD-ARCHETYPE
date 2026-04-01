---
## superpowers / README

### 來源
- repo：superpowers
- 路徑：README.md
- 檔案類型：README.md
- card_kind：overview
- language：en
- canonical_group：superpowers::readme.md

### 一句話定位
superpowers 的安裝、基本工作流、哲學與技能總覽入口。

### 核心人格特質
流程紀律, 完成導向, 規範導向, 元認知

### 核心思考框架
- [The Basic Workflow]
- 1. **brainstorming** - Activates before writing code. Refines rough ideas through questions, explores alternatives, presents design in sections for validation. Saves design document.
- 2. **using-git-worktrees** - Activates after design approval. Creates isolated workspace on new branch, runs project setup, verifies clean test baseline.
- 3. **writing-plans** - Activates with approved design. Breaks work into bite-sized tasks (2-5 minutes each). Every task has exact file paths, complete code, verification steps.
- 4. **subagent-driven-development** or **executing-plans** - Activates with plan. Dispatches fresh subagent per task with two-stage review (spec compliance, then code quality), or executes in batches with human checkpoints.
- 5. **test-driven-development** - Activates during implementation. Enforces RED-GREEN-REFACTOR: write failing test, watch it fail, write minimal code, watch it pass, commit. Deletes code written before tests.
- 6. **requesting-code-review** - Activates between tasks. Reviews against plan, reports issues by severity. Critical issues block progress.
- [Philosophy]
- - **Test-Driven Development** - Write tests first, always
- - **Systematic over ad-hoc** - Process over guessing
- - **Complexity reduction** - Simplicity as primary goal
- - **Evidence over claims** - Verify before declaring success

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- There's a bunch more to it, but that's the core of the system. And because the skills trigger automatically, you don't need to do anything special. Your coding agent just has Superpowers.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「superpowers 的安裝、基本工作流、哲學與技能總覽入口」的工作階段使用。
- 常見觸發語句：skills

### 原文精華摘錄
> Superpowers is a complete software development workflow for your coding agents, built on top of a set of composable "skills" and some initial instructions that make sure your agent uses them
> How it works
> It starts from the moment you fire up your coding agent. As soon as it sees that you're building something, it *doesn't* just jump into trying to write code. Instead, it steps back and asks you what you're really trying to do.
> Once it's teased a spec out of the conversation, it shows it to you in chunks short enough to actually read and digest.
> After you've signed off on the design, your agent puts together an implementation plan that's clear enough for an enthusiastic junior engineer with poor taste, no judgement, no project context, and an aversion to testing to follow. It emphasizes true red/gr...
> Next up, once you say "go", it launches a *subagent-driven-development* process, having agents work through each engineering task, inspecting and reviewing their work, and continuing forward. It's not uncommon for Claude to be able to work autonomously for...
> Sponsorship
> - Jesse
> Installation
> **Note:** Installation differs by platform. Claude Code or Cursor have built-in plugin marketplaces. Codex and OpenCode require manual setup.

### 和其他 skill 的潛在關聯
- README（marketingskills） - 互補 - 共享領域：meta；共享分類：流程型、元技能型；共同關鍵詞：agent, built, coding, contributing
- README（vercel-agent-skills） - 互補 - 共享領域：meta；共享分類：元技能型；共同關鍵詞：agent, coding, installation, instructions
- customer-research（marketingskills） - 互補 - 共享領域：meta；共享分類：流程型、元技能型；共同關鍵詞：community, support, top, what
- README（pm-skills） - 互補 - 共享領域：meta；共享分類：流程型、元技能型；共同關鍵詞：contributing, installation, license, works

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
