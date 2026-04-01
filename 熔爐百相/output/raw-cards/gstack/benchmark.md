---
## gstack / benchmark

### 來源
- repo：gstack
- 路徑：benchmark/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::benchmark/skill.md

### 一句話定位
以瀏覽守護程序建立與比較效能基線的性能檢測技能。

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 反思性, 累積導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- Only run `open` if the user says yes. Always run `touch` to mark as seen. This only happens once.
- When the user's request matches an available skill, ALWAYS invoke it using the Skill
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This command writes telemetry to
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
- auto-invoke skills based on conversation context. Only run skills the user explicitly
- If `LAKE_INTRO` is `no`: Before continuing, introduce the Completeness Principle.
- ask the user about telemetry. Use AskUserQuestion:
- ask the user about proactive behavior. Use AskUserQuestion:
- Use AskUserQuestion:

禁止做
- The user always has context you don't. Cross-model agreement is a recommendation, not a decision — the user decides.
- Slug: lowercase hyphens, max 60 chars. Skip if exists. Max 3/session. File inline, don't stop.
- - **Measure, don't guess.** Use actual performance.getEntries() data, not estimates.
- - **Read-only.** Produce the report. Don't modify code unless explicitly asked.
- This only happens once. If `TEL_PROMPTED` is `yes`, skip this entirely.
- This only happens once. If `PROACTIVE_PROMPTED` is `yes`, skip this entirely.
- This only happens once per project. If `HAS_ROUTING` is `yes` or `ROUTING_DECLINED` is `true`, skip this entirely.
- If you cannot determine the outcome, use "unknown". Both local JSONL and remote

### 提問方式
無明確提問模板

### 審查維度
- [GSTACK REVIEW REPORT]
- **VERDICT:** NO REVIEWS YET — run \`/autoplan\` for full review pipeline, or individual reviews above.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one

### 輸出格式要求
- [Plan Status Footer]
- When you are in plan mode and about to call ExitPlanMode:
- 1. Check if the plan file already has a `## GSTACK REVIEW REPORT` section.
- 2. If it DOES — skip (a review skill already wrote a richer report).
- 3. If it does NOT — run this command:
- Then write a `## GSTACK REVIEW REPORT` section to the end of the plan file:
- - If the output contains review entries (JSONL lines before `---CONFIG---`): format the
- [GSTACK REVIEW REPORT]
- **VERDICT:** NO REVIEWS YET — run \`/autoplan\` for full review pipeline, or individual reviews above.
- **PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one

### 適用場景
- 適合在需要「以瀏覽守護程序建立與比較效能基線的性能檢測技能」的工作階段使用。
- 常見觸發語句：performance / benchmark / page speed / lighthouse / web vitals / bundle size / load time

### 原文精華摘錄
> Performance regression detection using the browse daemon
> Establishes baselines for page load times, Core Web Vitals, and resource sizes
> Compares before/after on every PR
> Tracks performance trends over time
> Use when: "performance", "benchmark", "page speed", "lighthouse", "web vitals", "bundle size", "load time"
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
- seo-audit（marketingskills） - 相似 - 共享領域：browser, performance, review；共享分類：審查型、工具程序型；共同關鍵詞：core, page, speed, vitals
- aspnet-core（openai-skills） - 相似 - 共享領域：browser, performance, review；共享分類：審查型、流程型；共同關鍵詞：core, performance, web
- lead-magnets（marketingskills） - 相似 - 共享領域：browser, review；共享分類：審查型、工具程序型、流程型；共同關鍵詞：after, page, plan, resource
- react-best-practices（vercel-agent-skills） - 相似 - 共享領域：browser, performance, review；共享分類：審查型、工具程序型；共同關鍵詞：bundle, performance

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
