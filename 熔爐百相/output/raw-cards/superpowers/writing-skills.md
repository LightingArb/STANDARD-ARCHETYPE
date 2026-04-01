---
## superpowers / writing-skills

### 來源
- repo：superpowers
- 路徑：skills/writing-skills/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/writing-skills/skill.md

### 一句話定位
建立、測試與維護技能文件本身的技能設計技能。

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 規範導向, 元認知

### 核心思考框架
- [Overview]
- **Writing skills IS Test-Driven Development applied to process documentation.**
- **Personal skills live in agent-specific directories (`~/.claude/skills` for Claude Code, `~/.agents/skills/` for Codex)**
- **Core principle:** If you didn't watch an agent fail without the skill, you don't know if the skill teaches the right thing.
- **REQUIRED BACKGROUND:** You MUST understand superpowers:test-driven-development before using this skill. That skill defines the fundamental RED-GREEN-REFACTOR cycle. This skill adapts TDD to documentation.
- **Official guidance:** For Anthropic's official skill authoring best practices, see anthropic-best-practices.md. This document provides additional patterns and guidelines that complement the TDD-focused approach in this skill.
- [TDD Mapping for Skills]
- | TDD Concept | Skill Creation |
- |-------------|----------------|
- | **Test case** | Pressure scenario with subagent |
- | **Production code** | Skill document (SKILL.md) |
- | **Test fails (RED)** | Agent violates rule without skill (baseline) |
- | **Test passes (GREEN)** | Agent complies with skill present |
- What is this? Core principle in 1-2 sentences.
- [The Iron Law (Same as TDD)]
- **No exceptions:**
- - Not for "simple additions"
- - Not for "just adding a section"

### 核心行為規則
必須做
- **REQUIRED BACKGROUND:** You MUST understand superpowers:test-driven-development before using this skill. That skill defines the fundamental RED-GREEN-REFACTOR cycle. This skill adapts TDD to documentation.
- Always use subagents (50-100x context savings). REQUIRED: Use [other-skill-name] for workflow.
- - ✅ Good: `**REQUIRED SUB-SKILL:** Use superpowers:test-driven-development`
- - ✅ Good: `**REQUIRED BACKGROUND:** You MUST understand superpowers:systematic-debugging`
- **REQUIRED BACKGROUND:** The superpowers:test-driven-development skill explains why this matters. Same principles apply to documentation.
- **After writing ANY skill, you MUST STOP and complete the deployment process.**
- ## STOP: Before Moving to Next Skill

禁止做
- **Core principle:** If you didn't watch an agent fail without the skill, you don't know if the skill teaches the right thing.
- **Don't create for:**
- - **NEVER summarize the skill's process or workflow** (see CSO section for why)
- - **NEVER summarize the skill's process or workflow**
- - Don't repeat what's in cross-referenced skills
- - Don't explain what's obvious from command
- - Don't include multiple examples of same pattern
- **Never use flowcharts for:**
- **Don't:**
- - Don't keep untested changes as "reference"
- - Don't "adapt" while running tests
- Don't just state the rule - forbid specific workarounds:

### 提問方式
- - Academic questions: Do they understand the rules?
- - Pressure scenarios: Do they comply under stress?
- - Application scenarios: Can they apply the technique correctly?
- - Variation scenarios: Do they handle edge cases?
- - Missing information tests: Do instructions have gaps?
- - Recognition scenarios: Do they recognize when pattern applies?
- - Application scenarios: Can they use the mental model?
- - Counter-examples: Do they know when NOT to apply?
- - Retrieval scenarios: Can they find the right information?
- - Application scenarios: Can they use what they found correctly?
- - Gap testing: Are common use cases covered?
- - What choices did they make?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「建立、測試與維護技能文件本身的技能設計技能」的工作階段使用。
- 常出現在收尾、發布、分支整理或部署交接階段。

### 原文精華摘錄
> Use when creating new skills, editing existing skills, or verifying skills work before deployment
> Overview
> **Writing skills IS Test-Driven Development applied to process documentation.**
> **Personal skills live in agent-specific directories (`~/.claude/skills` for Claude Code, `~/.agents/skills/` for Codex)**
> **Core principle:** If you didn't watch an agent fail without the skill, you don't know if the skill teaches the right thing.
> **REQUIRED BACKGROUND:** You MUST understand superpowers:test-driven-development before using this skill. That skill defines the fundamental RED-GREEN-REFACTOR cycle. This skill adapts TDD to documentation.
> What is a Skill?
> **Skills are:** Reusable techniques, patterns, tools, reference guides
> **Skills are NOT:** Narratives about how you solved a problem once
> TDD Mapping for Skills
> | TDD Concept | Skill Creation |
> |-------------|----------------|
> | **Test case** | Pressure scenario with subagent |
> | **Production code** | Skill document (SKILL.md) |
> When to Create a Skill
> **Create when:**

### 和其他 skill 的潛在關聯
- plan-eng-review（gstack） - 相似 - 共享領域：planning, testing；共享分類：思考框架型、流程型；共同關鍵詞：file, next, optional, patterns
- ab-test-setup（marketingskills） - 相似 - 共享領域：meta, planning, testing；共享分類：思考框架型、流程型、元技能型；共同關鍵詞：common, core, mistakes, optimization
- paywall-upgrade-cro（marketingskills） - 相似 - 共享領域：meta, planning, testing；共享分類：思考框架型、流程型、元技能型；共同關鍵詞：anti-patterns, core, optimization, testing
- schema-markup（marketingskills） - 相似 - 共享領域：meta, testing；共享分類：思考框架型、流程型、元技能型；共同關鍵詞：common, core, optimization, quick

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
