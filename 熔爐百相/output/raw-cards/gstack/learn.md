---
## gstack / learn

### 來源
- repo：gstack
- 路徑：learn/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::learn/skill.md

### 一句話定位
管理專案長期 learnings，支援查詢、修剪與匯出的知識累積技能。

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 反思性, 累積導向

### 核心思考框架
- [Completeness Principle — Boil the Lake]
- **Effort reference** — always show both scales:

### 核心行為規則
必須做
- Only run `open` if the user says yes. Always run `touch` to mark as seen. This only happens once.
- When the user's request matches an available skill, ALWAYS invoke it using the Skill
- **ALWAYS follow this structure for every AskUserQuestion call:**
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This command writes telemetry to
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- auto-invoke skills based on conversation context. Only run skills the user explicitly
- If `LAKE_INTRO` is `no`: Before continuing, introduce the Completeness Principle.
- ask the user about telemetry. Use AskUserQuestion:
- ask the user about proactive behavior. Use AskUserQuestion:
- Use AskUserQuestion:
- The user wants to manually add a learning. Use AskUserQuestion to gather:

禁止做
- Quality matters. Bugs matter. Do not normalize sloppy software. Do not hand-wave away the last 1% or 5% of defects as acceptable. Great product aims at zero defects and takes edge cases seriously. Fix the whole thing, not just the demo path.
- **Humor:** dry observations about the absurdity of software. "This is a 200-line config file to print hello world." "The test suite takes longer than the feature it tests." Never forced, never self-referential about being AI.
- **User sovereignty.** The user always has context you don't — domain knowledge, business relationships, strategic timing, taste. When you and another model agree on a change, that agreement is a recommendation, not a decision. Present it. The user decides. Never say "the outside voice is right" and act. Say "the outside voice recommends X — do you want to proceed?"
- - Be direct about quality. "Well-designed" or "this is a mess." Don't dance around judgments.
- Slug: lowercase hyphens, max 60 chars. Skip if exists. Max 3/session. File inline, don't stop.
- This only happens once. If `TEL_PROMPTED` is `yes`, skip this entirely.
- This only happens once. If `PROACTIVE_PROMPTED` is `yes`, skip this entirely.
- This only happens once per project. If `HAS_ROUTING` is `yes` or `ROUTING_DECLINED` is `true`, skip this entirely.
- If you cannot determine the outcome, use "unknown". Both local JSONL and remote

### 提問方式
- **Final test:** does this sound like a real cross-functional builder who wants to help someone make something people want, ship it, and make it actually work?
- [AskUserQuestion Format]
- **ALWAYS follow this structure for every AskUserQuestion call:**
- 4. **Options:** Lettered options: `A) ... B) ... C) ...` — when an option involves effort, show both scales: `(human: ~X / CC: ~Y)`

### 審查維度
非審查型

### 輸出格式要求
- [AskUserQuestion Format]
- **ALWAYS follow this structure for every AskUserQuestion call:**
- 1. **Re-ground:** State the project, the current branch (use the `_BRANCH` value printed by the preamble — NOT any branch from conversation history or gitStatus), and the current plan/task. (1-2 sentences)
- 2. **Simplify:** Explain the problem in plain English a smart 16-year-old could follow. No raw function names, no internal jargon, no implementation details. Use concrete examples and analogies. Say what it DOES, not what it's called.
- 3. **Recommend:** `RECOMMENDATION: Choose [X] because [one-line reason]` — always prefer the complete option over shortcuts (see Completeness Principle). Include `Completeness: X/10` for each option. Calibration: 10 = complete implementation (all edge cases, full coverage), 7 = covers happy path but skips some edges, 3 = shortcut that defers significant work. If both options are 8+, pick the higher; if one is ≤5, flag it.
- 4. **Options:** Lettered options: `A) ... B) ... C) ...` — when an option involves effort, show both scales: `(human: ~X / CC: ~Y)`
- [Plan Status Footer]
- When you are in plan mode and about to call ExitPlanMode:
- 1. Check if the plan file already has a `## GSTACK REVIEW REPORT` section.
- 2. If it DOES — skip (a review skill already wrote a richer report).
- 3. If it does NOT — run this command:
- Then write a `## GSTACK REVIEW REPORT` section to the end of the plan file:
- - If the output contains review entries (JSONL lines before `---CONFIG---`): format the
- [GSTACK REVIEW REPORT]

### 適用場景
- 適合在需要「管理專案長期 learnings，支援查詢、修剪與匯出的知識累積技能」的工作階段使用。
- 常見觸發語句：what have we learned / show learnings / prune stale learnings / export learnings / didn't we fix this before?
- 也適合在使用者尚未明講，但上下文已顯示相同需求時主動建議使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Manage project learnings
> Review, search, prune, and export what gstack has learned across sessions
> Use when asked to "what have we learned", "show learnings", "prune stale learnings", or "export learnings"
> Proactively suggest when the user asks about past patterns or wonders "didn't we fix this before?"
> Preamble (run first)
> types (e.g., /qa, /ship). If you would have auto-invoked a skill, instead briefly say:
> Then offer to open the essay in their default browser:
> ask the user about telemetry. Use AskUserQuestion:
> Options:
> Skill routing
> When the user's request matches an available skill, ALWAYS invoke it using the Skill
> Key routing rules:
> - Product ideas, "is this worth building", brainstorming → invoke office-hours
> - Bugs, errors, "why is this broken", 500 errors → invoke investigate
> Voice
> **Core belief:** there is no one at the wheel. Much of the world is made up. That is not scary. That is the opportunity. Builders get to make new things real. Write in a way that makes capable people, especially young builders early in their careers, feel t...

### 和其他 skill 的潛在關聯
- README（ui-ux-pro-max-skill） - 相似 - 共享領域：learning, meta；共享分類：工具程序型、流程型；共同關鍵詞：across, command, what
- schema-markup（marketingskills） - 互補 - 共享領域：meta；共享分類：工具程序型、流程型；共同關鍵詞：add, fix, format, search
- cohort-analysis（pm-skills） - 相似 - 共享領域：learning, meta；共享分類：工具程序型、流程型；共同關鍵詞：format, patterns
- north-star-metric（pm-skills） - 相似 - 共享領域：learning, meta；共享分類：流程型；共同關鍵詞：about, what

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
