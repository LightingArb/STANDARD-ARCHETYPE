---
## superpowers / executing-plans

### 來源
- repo：superpowers
- 路徑：skills/executing-plans/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/executing-plans/skill.md

### 一句話定位
依既有實作計畫逐步施工、檢查與回報進度的執行技能。

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Overview]
- **Announce at start:** "I'm using the executing-plans skill to implement this plan."
- **Note:** Tell your human partner that Superpowers works much better with access to subagents. The quality of its work will be significantly higher if run on a platform with subagent support (such as Claude Code or Codex). If subagents are available, use superpowers:subagent-driven-development instead of this skill.
- [The Process]
- 1. Read plan file
- 2. Review critically - identify any questions or concerns about the plan
- 3. If concerns: Raise them with your human partner before starting
- 4. If no concerns: Create TodoWrite and proceed
- For each task:
- 1. Mark as in_progress

### 核心行為規則
必須做
- - **REQUIRED SUB-SKILL:** Use superpowers:finishing-a-development-branch
- - **superpowers:using-git-worktrees** - REQUIRED: Set up isolated workspace before starting

禁止做
- - You don't understand an instruction
- **Don't force through blockers** - stop and ask.
- - Don't skip verifications
- - Stop when blocked, don't guess
- - Never start implementation on main/master branch without explicit user consent

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「依既有實作計畫逐步施工、檢查與回報進度的執行技能」的工作階段使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Use when you have a written implementation plan to execute in a separate session with review checkpoints
> Overview
> **Announce at start:** "I'm using the executing-plans skill to implement this plan."
> **Note:** Tell your human partner that Superpowers works much better with access to subagents. The quality of its work will be significantly higher if run on a platform with subagent support (such as Claude Code or Codex). If subagents are available, use su...
> The Process
> 1. Read plan file
> 2. Review critically - identify any questions or concerns about the plan
> 3. If concerns: Raise them with your human partner before starting
> 4. If no concerns: Create TodoWrite and proceed
> When to Stop and Ask for Help

### 和其他 skill 的潛在關聯
- office-hours（gstack） - 相似 - 共享領域：planning；共同關鍵詞：have, help, plan, steps
- pricing-strategy（marketingskills） - 相似 - 共享領域：planning；共享分類：工具程序型、流程型；共同關鍵詞：help, overview, plan
- plan-ceo-review（gstack） - 相似 - 共享領域：planning；共享分類：流程型；共同關鍵詞：ask, plan, steps
- plan-design-review（gstack） - 相似 - 共享領域：planning；共享分類：流程型；共同關鍵詞：ask, plan, steps

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
