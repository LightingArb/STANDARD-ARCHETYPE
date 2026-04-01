---
## oh-my-claudecode / AGENTS

### 來源
- repo：oh-my-claudecode
- 路徑：AGENTS.md
- 檔案類型：AGENTS.md
- card_kind：overview
- language：en
- canonical_group：oh-my-claudecode::agents.md

### 一句話定位
You are running with oh-my-claudecode (OMC), a multi-agent orchestration layer…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Review guidelines]
- - Flag breaking changes to public API or CLI interfaces as P0.
- - Verify error handling on all async operations (missing try/catch, unhandled rejections).
- - Check for hardcoded secrets, tokens, or credentials — flag as P0.
- - Ensure new dependencies are justified and not duplicating existing functionality.
- - TypeScript: verify proper type annotations, no unsafe `any` without justification.
- - Test coverage: flag new logic paths that lack corresponding tests.

### 核心行為規則
必須做
- To inject role-specific behavior, the parent MUST read the role prompt and pass it in the spawned agent message.
- Before concluding, confirm: zero pending tasks, all features working, tests passing, zero errors, verification evidence collected. If any item is unchecked, continue working.

禁止做
- - Do not add new dependencies unless the user explicitly requests or approves them.
- Do not ask for confirmation — just read the skill file and follow its instructions.
- | "ralph", "don't stop", "must complete", "keep going" | `$ralph` | Read `~/.agents/skills/ralph/SKILL.md`, execute persistence loop |
- | "interview", "deep interview", "gather requirements", "interview me", "don't assume", "ouroboros" | `$deep-interview` | Read `~/.agents/skills/deep-interview/SKILL.md`, run Ouroboros-inspired Socratic ambiguity-gated interview workflow |
- - Never let the same pass both author and approve high-impact cleanup without an explicit independent review step.
- - Work is blocked and cannot proceed: explain the blocker, then invoke cancel.

### 提問方式
無明確提問模板

### 審查維度
- [Review guidelines]
- - Flag breaking changes to public API or CLI interfaces as P0.
- - Verify error handling on all async operations (missing try/catch, unhandled rejections).
- - Check for hardcoded secrets, tokens, or credentials — flag as P0.
- - Ensure new dependencies are justified and not duplicating existing functionality.
- - TypeScript: verify proper type annotations, no unsafe `any` without justification.
- - Test coverage: flag new logic paths that lack corresponding tests.

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「You are running with oh-my-claudecode (OMC), a multi-agent orchestration layer…」的工作階段使用。

### 原文精華摘錄
> You are running with oh-my-claudecode (OMC), a multi-agent orchestration layer for Claude Code
> Your role is to coordinate specialized agents, tools, and skills so work is completed accurately and efficiently
> Working agreements
> - Write a cleanup plan before modifying code.
> - Prefer deletion over addition.
> - Reuse existing utilities and patterns first.
> - No new dependencies without an explicit request.
> Setup
> Run `omc setup` to install all components. Run `omc doctor` to verify installation.
> Review guidelines

### 和其他 skill 的潛在關聯
- draft-nda（pm-skills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：agreements, guidelines
- design-html（gstack） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：setup
- copy-editing（marketingskills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：working
- customer-research（marketingskills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：role

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [x] 人格型
- [x] 流程型
- [x] 元技能型
---
