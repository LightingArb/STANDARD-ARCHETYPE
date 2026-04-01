---
## gstack / codex

### 來源
- repo：gstack
- 路徑：codex/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::codex/skill.md

### 一句話定位
把 OpenAI Codex 以 review、challenge、consult 三種模式接入工作流的包裝技能。

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向

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
- All prompts sent to Codex MUST be prefixed with this boundary instruction:
- root (`-C`) and cannot access `~/.claude/plans/` or any files outside the repo. You MUST
- auto-invoke skills based on conversation context. Only run skills the user explicitly
- If `LAKE_INTRO` is `no`: Before continuing, introduce the Completeness Principle.
- ask the user about telemetry. Use AskUserQuestion:
- ask the user about proactive behavior. Use AskUserQuestion:

禁止做
- Quality matters. Bugs matter. Do not normalize sloppy software. Do not hand-wave away the last 1% or 5% of defects as acceptable. Great product aims at zero defects and takes edge cases seriously. Fix the whole thing, not just the demo path.
- **Humor:** dry observations about the absurdity of software. "This is a 200-line config file to print hello world." "The test suite takes longer than the feature it tests." Never forced, never self-referential about being AI.
- **User sovereignty.** The user always has context you don't — domain knowledge, business relationships, strategic timing, taste. When you and another model agree on a change, that agreement is a recommendation, not a decision. Present it. The user decides. Never say "the outside voice is right" and act. Say "the outside voice recommends X — do you want to proceed?"
- - Be direct about quality. "Well-designed" or "this is a mess." Don't dance around judgments.
- - **`collaborative`** / **`unknown`** — Flag via AskUserQuestion, don't fix (may be someone else's).
- - **Layer 1** (tried and true) — don't reinvent. **Layer 2** (new and popular) — scrutinize. **Layer 3** (first principles) — prize above all.
- Slug: lowercase hyphens, max 60 chars. Skip if exists. Max 3/session. File inline, don't stop.
- **IMPORTANT — embed content, don't reference path:** Codex runs sandboxed to the repo
- - **Never modify files.** This skill is read-only. Codex runs in read-only sandbox mode.
- - **Present output verbatim.** Do not truncate, summarize, or editorialize Codex's output
- independent opinion. Do not re-run Claude Code's own review.
- This only happens once. If `TEL_PROMPTED` is `yes`, skip this entirely.

### 提問方式
- **Final test:** does this sound like a real cross-functional builder who wants to help someone make something people want, ship it, and make it actually work?
- [AskUserQuestion Format]
- **ALWAYS follow this structure for every AskUserQuestion call:**
- 4. **Options:** Lettered options: `A) ... B) ... C) ...` — when an option involves effort, show both scales: `(human: ~X / CC: ~Y)`

### 審查維度
- [GSTACK REVIEW REPORT]
- **VERDICT:** NO REVIEWS YET — run \`/autoplan\` for full review pipeline, or individual reviews above.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- [Step 2A: Review Mode]
- 1. Create temp files for output capture:
- 2. Run the review (5-minute timeout). **Always** pass the filesystem boundary instruction
- instructions, append them after the boundary separated by a newline:
- (e.g., `/codex review focus on security`), append them after the boundary:
- 3. Capture the output. Then parse cost from stderr:
- 4. Determine gate verdict by checking the review output for critical findings.
- [Plan File Review Report]
- **plan file** itself so review status is visible to anyone reading the plan.
- 1. Check if there is an active plan file in this conversation (the host provides plan file
- 2. If not found, skip this section silently — not every review runs in plan mode.
- Parse each JSONL entry. Each skill logs different fields:
- - **plan-ceo-review**: \`status\`, \`unresolved\`, \`critical_gaps\`, \`mode\`, \`scope_proposed\`, \`scope_accepted\`, \`scope_deferred\`, \`commit\`

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
- 適合在需要「把 OpenAI Codex 以 review、challenge、consult 三種模式接入工作流的包裝技能」的工作階段使用。
- 常見觸發語句：200 IQ autistic developer / codex review / codex challenge / ask codex / second opinion / consult codex
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> OpenAI Codex CLI wrapper — three modes
> Code review: independent diff review via codex review with pass/fail gate
> Challenge: adversarial mode that tries to break your code
> Consult: ask codex anything with session continuity for follow-ups
> The "200 IQ autistic developer" second opinion
> Use when asked to "codex review", "codex challenge", "ask codex", "second opinion", or "consult codex"
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

### 和其他 skill 的潛在關聯
- customer-research（marketingskills） - 相似 - 共享領域：meta, review；共享分類：審查型、工具程序型、流程型；共同關鍵詞：ask, modes, say, see
- seo-audit（marketingskills） - 相似 - 共享領域：meta, review；共享分類：審查型、工具程序型；共同關鍵詞：building, format, search, see
- xlsx（anthropics-skills） - 相似 - 共享領域：meta, review；共享分類：審查型、工具程序型、流程型；共同關鍵詞：file, important, report, something
- analytics-tracking（marketingskills） - 相似 - 共享領域：meta, review；共享分類：審查型、工具程序型、流程型；共同關鍵詞：format, plan, see, something

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
