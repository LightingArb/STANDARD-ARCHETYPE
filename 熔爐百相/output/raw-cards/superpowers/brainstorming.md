---
## superpowers / brainstorming

### 來源
- repo：superpowers
- 路徑：skills/brainstorming/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/brainstorming/skill.md

### 一句話定位
在任何創作或功能實作前先釐清意圖、需求與設計的前置腦暴技能。

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 品味導向, 體驗敏感

### 核心思考框架
- [Anti-Pattern: "This Is Too Simple To Need A Design"]
- Every project goes through this process. A todo list, a single-function utility, a config change — all of them. "Simple" projects are where unexamined assumptions cause the most wasted work. The design can be short (a few sentences for truly simple projects), but you MUST present it and get approval.
- [Checklist]
- You MUST create a task for each of these items and complete them in order:
- 1. **Explore project context** — check files, docs, recent commits
- 2. **Offer visual companion** (if topic will involve visual questions) — this is its own message, not combined with a clarifying question. See the Visual Companion section below.
- 3. **Ask clarifying questions** — one at a time, understand purpose/constraints/success criteria
- 4. **Propose 2-3 approaches** — with trade-offs and your recommendation
- 5. **Present design** — in sections scaled to their complexity, get user approval after each section
- [Process Flow]
- **The terminal state is invoking writing-plans.** Do NOT invoke frontend-design, mcp-builder, or any other implementation skill. The ONLY skill you invoke after brainstorming is writing-plans.
- [The Process]
- **Understanding the idea:**
- - Check out the current project state first (files, docs, recent commits)
- - Before asking detailed questions, assess scope: if the request describes multiple independent subsystems (e.g., "build a platform with chat, file storage, billing, and analytics"), flag this immediately. Don't spend questions refining details of a project that needs to be decomposed first.
- - If the project is too large for a single spec, help the user decompose into sub-projects: what are the independent pieces, how do they relate, what order should they be built? Then brainstorm the first sub-project through the normal design flow. Each sub-project gets its own spec → plan → implementation cycle.
- - For appropriately-scoped projects, ask questions one at a time to refine the idea
- - Prefer multiple choice questions when possible, but open-ended is fine too

### 核心行為規則
必須做
- Every project goes through this process. A todo list, a single-function utility, a config change — all of them. "Simple" projects are where unexamined assumptions cause the most wasted work. The design can be short (a few sentences for truly simple projects), but you MUST present it and get approval.
- You MUST create a task for each of these items and complete them in order:
- **This offer MUST be its own message.** Do not combine it with clarifying questions, context summaries, or any other content. The message should contain ONLY the offer above and nothing else. Wait for the user's response before continuing. If they decline, proceed with text-only brainstorming.
- - Before asking detailed questions, assess scope: if the request describes multiple independent subsystems (e.g., "build a platform with chat, file storage, billing, and analytics"), flag this immediately. Don't spend questions refining details of a project that needs to be decomposed first.

禁止做
- - Before asking detailed questions, assess scope: if the request describes multiple independent subsystems (e.g., "build a platform with chat, file storage, billing, and analytics"), flag this immediately. Don't spend questions refining details of a project that needs to be decomposed first.
- - Don't propose unrelated refactoring. Stay focused on what serves the current goal.
- - **One question at a time** - Don't overwhelm with multiple questions
- **This offer MUST be its own message.** Do not combine it with clarifying questions, context summaries, or any other content. The message should contain ONLY the offer above and nothing else. Wait for the user's response before continuing. If they decline, proceed with text-only brainstorming.

### 提問方式
- - For each unit, you should be able to answer: what does it do, how do you use it, and what does it depend on?
- 2. **Internal consistency:** Do any sections contradict each other? Does the architecture match the feature descriptions?
- 3. **Scope check:** Is this focused enough for a single implementation plan, or does it need decomposition?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「在任何創作或功能實作前先釐清意圖、需求與設計的前置腦暴技能」的工作階段使用。
- 常見觸發語句：You MUST use this before any creative work - creating features, building components, adding functionality, or modifying behavior. Explores user intent, requirements and design before implementation.
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> "You MUST use this before any creative work - creating features, building components, adding functionality, or modifying behavior
> Explores user intent, requirements and design before implementation."
> Anti-Pattern: "This Is Too Simple To Need A Design"
> Every project goes through this process. A todo list, a single-function utility, a config change — all of them. "Simple" projects are where unexamined assumptions cause the most wasted work. The design can be short (a few sentences for truly simple projects...
> Checklist
> You MUST create a task for each of these items and complete them in order:
> 1. **Explore project context** — check files, docs, recent commits
> 2. **Offer visual companion** (if topic will involve visual questions) — this is its own message, not combined with a clarifying question. See the Visual Companion section below.
> 3. **Ask clarifying questions** — one at a time, understand purpose/constraints/success criteria
> Process Flow

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, planning；共享分類：思考框架型、流程型；共同關鍵詞：adding, checklist, must, requirements
- plan-design-review（gstack） - 相似 - 共享領域：design, planning；共享分類：思考框架型、流程型；共同關鍵詞：after, building, principles, visual
- business-model（pm-skills） - 相似 - 共享領域：design, planning；共享分類：思考框架型、流程型；共同關鍵詞：building, creating, process, requirements
- product-strategy（pm-skills） - 相似 - 共享領域：design, planning；共享分類：思考框架型、流程型；共同關鍵詞：building, creating, process, requirements

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
