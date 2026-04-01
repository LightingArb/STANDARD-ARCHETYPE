---
## superpowers / writing-plans

### 來源
- repo：superpowers
- 路徑：skills/writing-plans/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/writing-plans/skill.md

### 一句話定位
在動手前先把多步驟工作拆成可執行計畫的規劃技能。

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向

### 核心思考框架
- [Overview]
- **Announce at start:** "I'm using the writing-plans skill to create the implementation plan."
- **Context:** This should be run in a dedicated worktree (created by brainstorming skill).
- **Save plans to:** `docs/superpowers/plans/YYYY-MM-DD-<feature-name>.md`
- - (User preferences for plan location override this default)

### 核心行為規則
必須做
- Before defining tasks, map out which files will be created or modified and what each one is responsible for. This is where decomposition decisions get locked in.
- **Every plan MUST start with this header:**
- > **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
- - **REQUIRED SUB-SKILL:** Use superpowers:subagent-driven-development
- - **REQUIRED SUB-SKILL:** Use superpowers:executing-plans

禁止做
- Assume they are a skilled developer, but know almost nothing about our toolset or problem domain. Assume they don't know good test design very well.
- - In existing codebases, follow established patterns. If the codebase uses large files, don't unilaterally restructure - but if a file you're modifying has grown unwieldy, including a split in the plan is reasonable.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「在動手前先把多步驟工作拆成可執行計畫的規劃技能」的工作階段使用。
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> Use when you have a spec or requirements for a multi-step task, before touching code
> Overview
> **Announce at start:** "I'm using the writing-plans skill to create the implementation plan."
> **Context:** This should be run in a dedicated worktree (created by brainstorming skill).
> **Save plans to:** `docs/superpowers/plans/YYYY-MM-DD-<feature-name>.md`
> - (User preferences for plan location override this default)
> Scope Check
> If the spec covers multiple independent subsystems, it should have been broken into sub-project specs during brainstorming. If it wasn't, suggest breaking this into separate plans — one per subsystem. Each plan should produce working, testable software on i...
> File Structure
> - Design units with clear boundaries and well-defined interfaces. Each file should have one clear responsibility.

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：planning；共享分類：思考框架型、流程型；共同關鍵詞：document, file, overview, requirements
- plan-ceo-review（gstack） - 相似 - 共享領域：planning；共享分類：思考框架型、流程型；共同關鍵詞：file, handoff, plan, scope
- plan-eng-review（gstack） - 相似 - 共享領域：planning；共享分類：思考框架型、流程型；共同關鍵詞：execution, file, plan, scope
- autoplan（gstack） - 相似 - 共享領域：planning；共享分類：流程型；共同關鍵詞：execution, file, plan, scope

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
