---
## gstack / land-and-deploy

### 來源
- repo：gstack
- 路徑：land-and-deploy/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::land-and-deploy/skill.md

### 一句話定位
接手 PR 合併、等待部署並做生產環境驗證的落地部署技能。

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Completeness Principle — Boil the Lake]
- **Effort reference** — always show both scales:
- [Non-interactive philosophy (like /ship) — with one critical gate]
- **Always stop for:**
- - **First-run dry-run validation (Step 1.5)** — shows deploy infrastructure and confirms setup
- - **Pre-merge readiness gate (Step 3.5)** — reviews, tests, docs check before merge
- - GitHub CLI not authenticated
- - No PR found for this branch
- - CI failures or merge conflicts

### 核心行為規則
必須做
- Only run `open` if the user says yes. Always run `touch` to mark as seen. This only happens once.
- When the user's request matches an available skill, ALWAYS invoke it using the Skill
- **ALWAYS follow this structure for every AskUserQuestion call:**
- Before building anything unfamiliar, **search first.** See `~/.claude/skills/gstack/ETHOS.md`.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This command writes telemetry to
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- auto-invoke skills based on conversation context. Only run skills the user explicitly
- If `LAKE_INTRO` is `no`: Before continuing, introduce the Completeness Principle.
- ask the user about telemetry. Use AskUserQuestion:
- ask the user about proactive behavior. Use AskUserQuestion:
- Use AskUserQuestion:
- ## Search Before Building

禁止做
- Quality matters. Bugs matter. Do not normalize sloppy software. Do not hand-wave away the last 1% or 5% of defects as acceptable. Great product aims at zero defects and takes edge cases seriously. Fix the whole thing, not just the demo path.
- **Humor:** dry observations about the absurdity of software. "This is a 200-line config file to print hello world." "The test suite takes longer than the feature it tests." Never forced, never self-referential about being AI.
- **User sovereignty.** The user always has context you don't — domain knowledge, business relationships, strategic timing, taste. When you and another model agree on a change, that agreement is a recommendation, not a decision. Present it. The user decides. Never say "the outside voice is right" and act. Say "the outside voice recommends X — do you want to proceed?"
- - Be direct about quality. "Well-designed" or "this is a mess." Don't dance around judgments.
- - **`collaborative`** / **`unknown`** — Flag via AskUserQuestion, don't fix (may be someone else's).
- - **Layer 1** (tried and true) — don't reinvent. **Layer 2** (new and popular) — scrutinize. **Layer 3** (first principles) — prize above all.
- Slug: lowercase hyphens, max 60 chars. Skip if exists. Max 3/session. File inline, don't stop.
- **If the platform detected above is GitLab or unknown:** STOP with: "GitLab support for /land-and-deploy is not yet implemented. Run `/ship` to create the MR, then merge manually via the GitLab web UI." Do not proceed.
- **Never stop for:**
- - **Never be robotic.** "I ran 4 checks and found 1 issue" not "CHECKS: 4, ISSUES: 1."
- If the merge fails with a permission error: **STOP.** "I don't have permission to merge this PR. You'll need a maintainer to merge it, or check your repo's branch protection rules."
- - Tell the user: "PR merged. I don't see a deploy workflow — your project might deploy a different way, or it might be a library/CLI that doesn't have a deploy step. I'll figure out the right verification in the next step."

### 提問方式
- **Final test:** does this sound like a real cross-functional builder who wants to help someone make something people want, ship it, and make it actually work?
- [AskUserQuestion Format]
- **ALWAYS follow this structure for every AskUserQuestion call:**
- 4. **Options:** Lettered options: `A) ... B) ... C) ...` — when an option involves effort, show both scales: `(human: ~X / CC: ~Y)`
- [Step 10: Suggest follow-ups]

### 審查維度
- [GSTACK REVIEW REPORT]
- **VERDICT:** NO REVIEWS YET — run \`/autoplan\` for full review pipeline, or individual reviews above.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- Merges the PR, waits for CI and deploy, verifies production health via canary checks

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
- 適合在需要「接手 PR 合併、等待部署並做生產環境驗證的落地部署技能」的工作階段使用。
- 常見觸發語句：merge / land / deploy / merge and verify / land it / ship it to production
- 常出現在收尾、發布、分支整理或部署交接階段。

### 原文精華摘錄
> Land and deploy workflow
> Merges the PR, waits for CI and deploy, verifies production health via canary checks
> Takes over after /ship creates the PR
> Use when: "merge", "land", "deploy", "merge and verify", "land it", "ship it to production"
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
- seo-audit（marketingskills） - 相似 - 共享領域：browser, review, shipping；共享分類：審查型、工具程序型；共同關鍵詞：building, format, health, like
- lead-magnets（marketingskills） - 相似 - 共享領域：browser, review, shipping；共享分類：審查型、工具程序型、流程型；共同關鍵詞：after, format, plan, see
- programmatic-seo（marketingskills） - 相似 - 共享領域：browser, review, shipping；共享分類：審查型、工具程序型；共同關鍵詞：building, checks, format, see
- customer-research（marketingskills） - 相似 - 共享領域：browser, review, shipping；共享分類：審查型、工具程序型、流程型；共同關鍵詞：say, see, voice, what

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
