---
## gstack / ship

### 來源
- repo：gstack
- 路徑：ship/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::ship/skill.md

### 一句話定位
從合併 base、跑測試、更新版本到推分支建 PR 的完整出貨技能。

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 證據導向, 懷疑論

### 核心思考框架
- [Completeness Principle — Boil the Lake]
- **Effort reference** — always show both scales:

### 核心行為規則
必須做
- Only run `open` if the user says yes. Always run `touch` to mark as seen. This only happens once.
- When the user's request matches an available skill, ALWAYS invoke it using the Skill
- **ALWAYS follow this structure for every AskUserQuestion call:**
- Before building anything unfamiliar, **search first.** See `~/.claude/skills/gstack/ETHOS.md`.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This command writes telemetry to
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- Before analyzing coverage, detect the project's test framework:
- Before proceeding, check CLAUDE.md for a `## Test Coverage` section with `Minimum:` and `Target:` fields. If found, use those percentages. Otherwise use defaults: Minimum = 60%, Target = 80%.
- Before invoking browse-based verification, check if a dev server is reachable:
- Before reviewing code quality, check: **did they build what was requested — nothing more, nothing less?**
- Every finding MUST include a confidence score (1-10):
- Before replying to any comment, run the **Escalation Detection** algorithm from greptile-triage.md to determine whether to use Tier 1 (friendly) or Tier 2 (firm) reply templates.

禁止做
- Quality matters. Bugs matter. Do not normalize sloppy software. Do not hand-wave away the last 1% or 5% of defects as acceptable. Great product aims at zero defects and takes edge cases seriously. Fix the whole thing, not just the demo path.
- **Humor:** dry observations about the absurdity of software. "This is a 200-line config file to print hello world." "The test suite takes longer than the feature it tests." Never forced, never self-referential about being AI.
- **User sovereignty.** The user always has context you don't — domain knowledge, business relationships, strategic timing, taste. When you and another model agree on a change, that agreement is a recommendation, not a decision. Present it. The user decides. Never say "the outside voice is right" and act. Say "the outside voice recommends X — do you want to proceed?"
- - Be direct about quality. "Well-designed" or "this is a mess." Don't dance around judgments.
- - **`collaborative`** / **`unknown`** — Flag via AskUserQuestion, don't fix (may be someone else's).
- - **Layer 1** (tried and true) — don't reinvent. **Layer 2** (new and popular) — scrutinize. **Layer 3** (first principles) — prize above all.
- Slug: lowercase hyphens, max 60 chars. Skip if exists. Max 3/session. File inline, don't stop.
- **Never stop for:**
- - **Eng Review (required by default):** The only review that gates shipping. Covers architecture, code quality, tests, performance. Can be disabled globally with \`gstack-config set skip_eng_review true\` (the "don't bother me" setting).
- - **Outside Voice (optional):** Independent plan review from a different AI model. Offered after all review sections complete in /plan-ceo-review and /plan-eng-review. Falls back to Claude subagent if Codex is unavailable. Never gates shipping.
- C) Skip — don't set up testing right now
- 3. **For each file:** Write one test that tests real behavior with meaningful assertions. Never `expect(x).toBeDefined()` — test what the code DOES.

### 提問方式
- **Final test:** does this sound like a real cross-functional builder who wants to help someone make something people want, ship it, and make it actually work?
- [AskUserQuestion Format]
- **ALWAYS follow this structure for every AskUserQuestion call:**
- 4. **Options:** Lettered options: `A) ... B) ... C) ...` — when an option involves effort, show both scales: `(human: ~X / CC: ~Y)`
- - Every edge: what happens with null input? Empty array? Invalid type?
- - **Interaction edge cases:** What happens when the user does something unexpected?
- - **Error states the user can see:** For every error the code handles, what does the user actually experience?
- - Is there a clear error message or a silent failure?
- - Can the user recover (retry, go back, fix input) or are they stuck?
- - What happens with no network? With a 500 from the API? With invalid data from the server?
- - **Empty/zero/boundary states:** What does the UI show with zero results? With 10,000 results? With a single character input? With maximum-length input?
- 2. Identify the **stated intent** — what was this branch supposed to accomplish?

### 審查維度
- [GSTACK REVIEW REPORT]
- **VERDICT:** NO REVIEWS YET — run \`/autoplan\` for full review pipeline, or individual reviews above.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- [Review Readiness Dashboard]
- **Source attribution:** If the most recent entry for a skill has a \`"via"\` field, append it to the status label in parentheses. Examples: `plan-eng-review` with `via:"autoplan"` shows as "CLEAR (PLAN via /autoplan)". `review` with `via:"ship"` shows as "CLEAR (DIFF via /ship)". Entries without a `via` field show as "CLEAR (PLAN)" or "CLEAR (DIFF)" as before.
- Display:
- **Review tiers:**
- - **Eng Review (required by default):** The only review that gates shipping. Covers architecture, code quality, tests, performance. Can be disabled globally with \`gstack-config set skip_eng_review true\` (the "don't bother me" setting).
- - **CEO Review (optional):** Use your judgment. Recommend it for big product/business changes, new user-facing features, or scope decisions. Skip for bug fixes, refactors, infra, and cleanup.
- - **Design Review (optional):** Use your judgment. Recommend it for UI/UX changes. Skip for backend-only, infra, or prompt-only changes.
- [Step 3.4: Test Coverage Audit]
- Before analyzing coverage, detect the project's test framework:
- 1. **Read CLAUDE.md** — look for a `## Testing` section with test command and framework name. If found, use that as the authoritative source.
- 2. **If CLAUDE.md has no testing section, auto-detect:**
- 3. **If no framework detected:** falls through to the Test Framework Bootstrap step (Step 2.5) which handles full setup.
- **0. Before/after test count:**

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
- 適合在需要「從合併 base、跑測試、更新版本到推分支建 PR 的完整出貨技能」的工作階段使用。
- 常見觸發語句：ship / deploy / push to main / create a PR / merge and push / get it deployed
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Ship workflow: detect + merge base branch, run tests, review diff, bump VERSION, update CHANGELOG, commit, push, create PR
> Use when asked to "ship", "deploy", "push to main", "create a PR", "merge and push", or "get it deployed"
> Proactively invoke this skill (do NOT push/PR directly) when the user says code is ready, asks about deploying, wants to push code up, or asks to create a PR
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
> AskUserQuestion Format

### 和其他 skill 的潛在關聯
- analytics-tracking（marketingskills） - 相似 - 共享領域：review, shipping, testing；共享分類：審查型、工具程序型、流程型；共同關鍵詞：asks, audit, format, framework
- seo-audit（marketingskills） - 相似 - 共享領域：review, shipping；共享分類：審查型、工具程序型；共同關鍵詞：audit, building, format, framework
- schema-markup（marketingskills） - 相似 - 共享領域：review, shipping, testing；共享分類：審查型、工具程序型、流程型；共同關鍵詞：format, pages, results, search
- ab-test-setup（marketingskills） - 相似 - 共享領域：shipping, testing；共享分類：審查型、工具程序型、流程型；共同關鍵詞：framework, metrics, plan, results

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
