---
## gstack / review

### 來源
- repo：gstack
- 路徑：review/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::review/skill.md

### 一句話定位
在合併前對 diff 做結構性風險審查並盡可能先修問題的 PR review 技能。

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 謹慎, 防呆導向

### 核心思考框架
- [Completeness Principle — Boil the Lake]
- **Effort reference** — always show both scales:
- [Step 2: Read the checklist]
- **If the file cannot be read, STOP and report the error.** Do not proceed without the checklist.

### 核心行為規則
必須做
- Only run `open` if the user says yes. Always run `touch` to mark as seen. This only happens once.
- When the user's request matches an available skill, ALWAYS invoke it using the Skill
- **ALWAYS follow this structure for every AskUserQuestion call:**
- Before building anything unfamiliar, **search first.** See `~/.claude/skills/gstack/ETHOS.md`.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This command writes telemetry to
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- Before reviewing code quality, check: **did they build what was requested — nothing more, nothing less?**
- Every finding MUST include a confidence score (1-10):
- Before producing the final review output:
- Before replying to any comment, run the **Escalation Detection** algorithm from greptile-triage.md to determine whether to use Tier 1 (friendly) or Tier 2 (firm) reply templates.
- auto-invoke skills based on conversation context. Only run skills the user explicitly
- If `LAKE_INTRO` is `no`: Before continuing, introduce the Completeness Principle.

禁止做
- Quality matters. Bugs matter. Do not normalize sloppy software. Do not hand-wave away the last 1% or 5% of defects as acceptable. Great product aims at zero defects and takes edge cases seriously. Fix the whole thing, not just the demo path.
- **Humor:** dry observations about the absurdity of software. "This is a 200-line config file to print hello world." "The test suite takes longer than the feature it tests." Never forced, never self-referential about being AI.
- **User sovereignty.** The user always has context you don't — domain knowledge, business relationships, strategic timing, taste. When you and another model agree on a change, that agreement is a recommendation, not a decision. Present it. The user decides. Never say "the outside voice is right" and act. Say "the outside voice recommends X — do you want to proceed?"
- - Be direct about quality. "Well-designed" or "this is a mess." Don't dance around judgments.
- - **`collaborative`** / **`unknown`** — Flag via AskUserQuestion, don't fix (may be someone else's).
- - **Layer 1** (tried and true) — don't reinvent. **Layer 2** (new and popular) — scrutinize. **Layer 3** (first principles) — prize above all.
- Slug: lowercase hyphens, max 60 chars. Skip if exists. Max 3/session. File inline, don't stop.
- You are running the `/review` workflow. Analyze the current branch's diff against the base branch for structural issues that tests don't catch.
- - **Items in the diff that don't match any plan item** become evidence for **SCOPE CREEP** detection.
- **If the file cannot be read, STOP and report the error.** Do not proceed without the checklist.
- Follow the output format specified in the checklist. Respect the suppressions — do NOT flag items listed in the "DO NOT flag" section.
- Do not output anything else — no preamble, no summary, no commentary.

### 提問方式
- **Final test:** does this sound like a real cross-functional builder who wants to help someone make something people want, ship it, and make it actually work?
- [AskUserQuestion Format]
- **ALWAYS follow this structure for every AskUserQuestion call:**
- 4. **Options:** Lettered options: `A) ... B) ... C) ...` — when an option involves effort, show both scales: `(human: ~X / CC: ~Y)`
- 2. Identify the **stated intent** — what was this branch supposed to accomplish?

### 審查維度
- [GSTACK REVIEW REPORT]
- **VERDICT:** NO REVIEWS YET — run \`/autoplan\` for full review pipeline, or individual reviews above.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- [Implementation Items]
- [DONE] Create UserService — src/services/user_service.rb (+142 lines)
- [PARTIAL] Add validation — model validates but missing controller checks
- [NOT DONE] Add caching layer — no cache-related changes in diff
- [CHANGED] "Redis queue" → implemented with Sidekiq instead
- [Test Items]
- [DONE] Unit tests for UserService — test/services/user_service_test.rb
- [NOT DONE] E2E test for signup flow
- [Migration Items]
- [DONE] Create users table — db/migrate/20240315_create_users.rb
- ─────────────────────────────────
- COMPLETION: 4/7 DONE, 1 PARTIAL, 1 NOT DONE, 1 CHANGED
- DISCREPANCY: {PARTIAL|NOT_DONE} | {plan item} | {what was actually delivered}

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
- 適合在需要「在合併前對 diff 做結構性風險審查並盡可能先修問題的 PR review 技能」的工作階段使用。
- 常見觸發語句：review this PR / code review / pre-landing review / check my diff
- 也適合在使用者尚未明講，但上下文已顯示相同需求時主動建議使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Pre-landing PR review
> Analyzes diff against the base branch for SQL safety, LLM trust boundary violations, conditional side effects, and other structural issues
> Use when asked to "review this PR", "code review", "pre-landing review", or "check my diff"
> Proactively suggest when the user is about to merge or land code changes
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
> Voice
> **Core belief:** there is no one at the wheel. Much of the world is made up. That is not scary. That is the opportunity. Builders get to make new things real. Write in a way that makes capable people, especially young builders early in their careers, feel t...
> **Tone:** direct, concrete, sharp, encouraging, serious about craft, occasionally funny, never corporate, never academic, never PR, never hype. Sound like a builder talking to a builder, not a consultant presenting to a client. Match the context: YC partner...
> **Humor:** dry observations about the absurdity of software. "This is a 200-line config file to print hello world." "The test suite takes longer than the feature it tests." Never forced, never self-referential about being AI.
> **Concreteness is the standard.** Name the file, the function, the line number. Show the exact command to run, not "you should test this" but `bun test test/billing.test.ts`. When explaining a tradeoff, use real numbers: not "this might be slow" but "this q...

### 和其他 skill 的潛在關聯
- analytics-tracking（marketingskills） - 相似 - 共享領域：review, shipping, testing；共享分類：思考框架型、審查型、流程型；共同關鍵詞：core, format, plan, see
- schema-markup（marketingskills） - 相似 - 共享領域：review, shipping, testing；共享分類：思考框架型、審查型、流程型；共同關鍵詞：core, format, issues, search
- develop-web-game（openai-skills） - 相似 - 共享領域：review, shipping, testing；共享分類：思考框架型、審查型、流程型；共同關鍵詞：building, changes, checklist, core
- xlsx（anthropics-skills） - 相似 - 共享領域：review, shipping；共享分類：思考框架型、審查型、流程型；共同關鍵詞：checklist, critical, important, other

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
