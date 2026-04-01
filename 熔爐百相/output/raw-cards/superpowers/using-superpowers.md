---
## superpowers / using-superpowers

### 來源
- repo：superpowers
- 路徑：skills/using-superpowers/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/using-superpowers/skill.md

### 一句話定位
在任何對話開始時先找對 skill 並嚴格遵守技能優先級的總入口技能。

### 核心人格特質
流程紀律, 完成導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- If you think there is even a 1% chance a skill might apply to what you are doing, you ABSOLUTELY MUST invoke the skill.
- IF A SKILL APPLIES TO YOUR TASK, YOU DO NOT HAVE A CHOICE. YOU MUST USE IT.

禁止做
- IF A SKILL APPLIES TO YOUR TASK, YOU DO NOT HAVE A CHOICE. YOU MUST USE IT.
- If CLAUDE.md, GEMINI.md, or AGENTS.md says "don't use TDD" and a skill says "always use TDD," follow the user's instructions. The user is in control.
- **In Claude Code:** Use the `Skill` tool. When you invoke a skill, its content is loaded and presented to you—follow it directly. Never use the Read tool on skill files.
- **Invoke relevant or requested skills BEFORE any response or action.** Even a 1% chance a skill might apply means that you should invoke the skill to check. If an invoked skill turns out to be wrong for the situation, you don't need to use it.
- **Rigid** (TDD, debugging): Follow exactly. Don't adapt away discipline.
- If you were dispatched as a subagent to execute a specific task, skip this skill.
- This is not negotiable. This is not optional. You cannot rationalize your way out of this.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「在任何對話開始時先找對 skill 並嚴格遵守技能優先級的總入口技能」的工作階段使用。

### 原文精華摘錄
> Use when starting any conversation - establishes how to find and use skills, requiring Skill tool invocation before ANY response including clarifying questions
> Instruction Priority
> Superpowers skills override default system prompt behavior, but **user instructions always take precedence**:
> 1. **User's explicit instructions** (CLAUDE.md, GEMINI.md, AGENTS.md, direct requests) — highest priority
> 2. **Superpowers skills** — override default system behavior where they conflict
> 3. **Default system prompt** — lowest priority
> How to Access Skills
> **In Claude Code:** Use the `Skill` tool. When you invoke a skill, its content is loaded and presented to you—follow it directly. Never use the Read tool on skill files.
> **In Copilot CLI:** Use the `skill` tool. Skills are auto-discovered from installed plugins. The `skill` tool works the same as Claude Code's `Skill` tool.
> **In Gemini CLI:** Skills activate via the `activate_skill` tool. Gemini loads skill metadata at session start and activates the full content on demand.

### 和其他 skill 的潛在關聯
- plan-ceo-review（gstack） - 互補 - 共享分類：流程型；共同關鍵詞：find, platform, priority, questions
- ai-seo（marketingskills） - 互補 - 共享領域：meta；共享分類：流程型、元技能型；共同關鍵詞：questions, starting, tool, types
- paid-ads（marketingskills） - 互補 - 共享領域：meta；共享分類：流程型、元技能型；共同關鍵詞：platform, questions, starting, tool
- autoplan（gstack） - 互補 - 共享領域：meta；共享分類：流程型、元技能型；共同關鍵詞：instructions, platform, questions

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
