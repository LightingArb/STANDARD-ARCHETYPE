---
## gstack / setup-browser-cookies

### 來源
- repo：gstack
- 路徑：setup-browser-cookies/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::setup-browser-cookies/skill.md

### 一句話定位
把真實 Chromium 的 cookies 匯入 headless 瀏覽 session 的登入前置技能。

### 核心人格特質
務實, 操作導向, 證據導向, 懷疑論

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
- This only happens once. If `TEL_PROMPTED` is `yes`, skip this entirely.
- This only happens once. If `PROACTIVE_PROMPTED` is `yes`, skip this entirely.
- This only happens once per project. If `HAS_ROUTING` is `yes` or `ROUTING_DECLINED` is `true`, skip this entirely.
- If you cannot determine the outcome, use "unknown". Both local JSONL and remote

### 提問方式
無明確提問模板

### 審查維度
非審查型

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
- 適合在需要「把真實 Chromium 的 cookies 匯入 headless 瀏覽 session 的登入前置技能」的工作階段使用。
- 常見觸發語句：import cookies / login to the site / authenticate the browser
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Import cookies from your real Chromium browser into the headless browse session
> Opens an interactive picker UI where you select which cookie domains to import
> Use before QA testing authenticated pages
> Use when asked to "import cookies", "login to the site", or "authenticate the browser"
> (gstack)
> Preamble (run first)
> types (e.g., /qa, /ship). If you would have auto-invoked a skill, instead briefly say:
> Then offer to open the essay in their default browser:
> ask the user about telemetry. Use AskUserQuestion:
> Options:

### 和其他 skill 的潛在關聯
- paywall-upgrade-cro（marketingskills） - 相似 - 共享領域：browser；共享分類：工具程序型；共同關鍵詞：pages, plan, testing, where
- site-architecture（marketingskills） - 相似 - 共享領域：browser；共享分類：工具程序型；共同關鍵詞：pages, plan, site, what
- pdf（anthropics-skills） - 相似 - 共享領域：browser；共享分類：工具程序型；共同關鍵詞：make, pages, steps
- copywriting（marketingskills） - 相似 - 共享領域：browser；共享分類：工具程序型；共同關鍵詞：make, pages, voice

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [ ] 流程型
- [ ] 元技能型
---
