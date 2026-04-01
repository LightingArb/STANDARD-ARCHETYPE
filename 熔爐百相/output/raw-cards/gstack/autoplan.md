---
## gstack / autoplan

### 來源
- repo：gstack
- 路徑：autoplan/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::autoplan/skill.md

### 一句話定位
自動串接 CEO、設計與工程評審，按原則連續審核方案的總控技能。

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 規範導向, 元認知, 品味導向, 體驗敏感, 挑戰性

### 核心思考框架
- [Completeness Principle — Boil the Lake]
- **Effort reference** — always show both scales:
- [The 6 Decision Principles]
- These rules auto-answer every intermediate question:
- 1. **Choose completeness** — Ship the whole thing. Pick the approach that covers more edge cases.
- 2. **Boil lakes** — Fix everything in the blast radius (files modified by this plan + direct importers). Auto-approve expansions that are in blast radius AND < 1 day CC effort (< 5 files, no new infra).
- 3. **Pragmatic** — If two options fix the same thing, pick the cleaner one. 5 seconds choosing, not 5 minutes.
- 4. **DRY** — Duplicates existing functionality? Reject. Reuse what exists.
- 5. **Explicit over clever** — 10-line obvious fix > 200-line abstraction. Pick what a new contributor reads in 30 seconds.
- [Decision Classification]
- Every auto-decision is classified:
- **Mechanical** — one clearly right answer. Auto-decide silently.
- **Taste** — reasonable people could disagree. Auto-decide with recommendation, but surface at the final gate. Three natural sources:
- 1. **Close approaches** — top two are both viable with different tradeoffs.
- 2. **Borderline scope** — in blast radius but 3-5 files, or ambiguous radius.
- [Phase 4: Final Approval Gate]
- **STOP here and present the final state to the user.**
- Present as a message, then use AskUserQuestion:

### 核心行為規則
必須做
- Only run `open` if the user says yes. Always run `touch` to mark as seen. This only happens once.
- When the user's request matches an available skill, ALWAYS invoke it using the Skill
- **ALWAYS follow this structure for every AskUserQuestion call:**
- Before building anything unfamiliar, **search first.** See `~/.claude/skills/gstack/ETHOS.md`.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This command writes telemetry to
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- Phases MUST execute in strict order: CEO → Design → Eng.
- Each phase MUST complete fully before the next begins.
- **You MUST still:**
- **You MUST NOT:**
- All prompts sent to Codex (via `codex exec` or `codex review`) MUST be prefixed with
- Before doing anything, save the plan file's current state to an external file:

禁止做
- Quality matters. Bugs matter. Do not normalize sloppy software. Do not hand-wave away the last 1% or 5% of defects as acceptable. Great product aims at zero defects and takes edge cases seriously. Fix the whole thing, not just the demo path.
- **Humor:** dry observations about the absurdity of software. "This is a 200-line config file to print hello world." "The test suite takes longer than the feature it tests." Never forced, never self-referential about being AI.
- **User sovereignty.** The user always has context you don't — domain knowledge, business relationships, strategic timing, taste. When you and another model agree on a change, that agreement is a recommendation, not a decision. Present it. The user decides. Never say "the outside voice is right" and act. Say "the outside voice recommends X — do you want to proceed?"
- - Be direct about quality. "Well-designed" or "this is a mess." Don't dance around judgments.
- - **`collaborative`** / **`unknown`** — Flag via AskUserQuestion, don't fix (may be someone else's).
- - **Layer 1** (tried and true) — don't reinvent. **Layer 2** (new and popular) — scrutinize. **Layer 3** (first principles) — prize above all.
- Slug: lowercase hyphens, max 60 chars. Skip if exists. Max 3/session. File inline, don't stop.
- /office-hours first next time." Then proceed normally. Do not re-offer later in the session.
- 6. **Bias toward action** — Merge > review cycles > stale deliberation. Flag concerns but don't block.
- the user specified, this is a User Challenge. It is NEVER auto-decided.
- NEVER run phases in parallel — each builds on the previous.
- was flagged. NEVER compress a section to just its name in a table row.

### 提問方式
- **Final test:** does this sound like a real cross-functional builder who wants to help someone make something people want, ship it, and make it actually work?
- [AskUserQuestion Format]
- **ALWAYS follow this structure for every AskUserQuestion call:**
- 4. **Options:** Lettered options: `A) ... B) ... C) ...` — when an option involves effort, show both scales: `(human: ~X / CC: ~Y)`
- 1. Is this the right problem to solve? Could a reframing yield 10x impact?
- 2. Are the premises stated or just assumed? Which ones could be wrong?
- 3. What's the 6-month regret scenario — what will look foolish?
- 4. What alternatives were dismissed without sufficient analysis?
- 5. What's the competitive risk — could someone else solve this first/better?
- 1. Information hierarchy: what does the user see first, second, third? Is it right?
- 2. Missing states: loading, empty, error, success, partial — which are unspecified?
- 3. User journey: what's the emotional arc? Where does it break?

### 審查維度
- [GSTACK REVIEW REPORT]
- **VERDICT:** NO REVIEWS YET — run \`/autoplan\` for full review pipeline, or individual reviews above.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- [Phase 1: CEO Review (Strategy & Scope)]
- **Override rules:**
- - Mode selection: SELECTIVE EXPANSION
- - Premises: accept reasonable ones (P6), challenge only clearly wrong ones
- - **GATE: Present premises to user for confirmation** — this is the ONE AskUserQuestion
- - Alternatives: pick highest completeness (P1). If tied, pick simplest (P5).
- - Scope expansion: in blast radius + <1d CC → approve (P2). Outside → defer to TODOS.md (P3).
- [Phase 2: Design Review (conditional — skip if no UI scope)]
- - Focus areas: all relevant dimensions (P1)
- - Structural issues (missing states, broken hierarchy): auto-fix (P5)
- - Aesthetic/taste issues: mark TASTE DECISION
- - Design system alignment: auto-fix if DESIGN.md exists and fix is obvious
- - Dual voices: always run BOTH Claude subagent AND Codex if available (P6).

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
- 適合在需要「自動串接 CEO、設計與工程評審，按原則連續審核方案的總控技能」的工作階段使用。
- 常見觸發語句：auto review / autoplan / run all reviews / review this plan
automatically / make the decisions for me
- 也適合在使用者尚未明講，但上下文已顯示相同需求時主動建議使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Auto-review pipeline — reads the full CEO, design, and eng review skills from disk and runs them sequentially with auto-decisions using 6 decision principles
> Surfaces taste decisions (close approaches, borderline scope, codex disagreements) at a final approval gate
> One command, fully reviewed plan out
> Use when asked to "auto review", "autoplan", "run all reviews", "review this plan automatically", or "make the decisions for me"
> Proactively suggest when the user has a plan file and wants to run the full review gauntlet without answering 15-30 intermediate questions
> (gstack)
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

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：meta, planning, review；共享分類：審查型、流程型、元技能型；共同關鍵詞：all, file, important, means
- analytics-tracking（marketingskills） - 相似 - 共享領域：meta, planning, review；共享分類：審查型、流程型、元技能型；共同關鍵詞：audit, format, plan, principles
- free-tool-strategy（marketingskills） - 相似 - 共享領域：meta, planning, review；共享分類：審查型、流程型、元技能型；共同關鍵詞：audit, plan, principles, questions
- lead-magnets（marketingskills） - 相似 - 共享領域：meta, planning, review；共享分類：審查型、流程型、元技能型；共同關鍵詞：format, offer, plan, principles

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
