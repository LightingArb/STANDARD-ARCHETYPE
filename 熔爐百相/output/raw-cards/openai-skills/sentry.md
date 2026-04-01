---
## openai-skills / sentry

### 來源
- repo：openai-skills
- 路徑：skills/.curated/sentry/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/sentry/skill.md

### 一句話定位
"Use when the user asks to inspect Sentry issues or events, summarize recent pr…

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Never ask the user to paste the full token in chat. Ask them to set it locally and confirm when ready.
- - Redact PII in output (emails, IPs). Do not print raw stack traces.
- - Never echo auth tokens.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
- [Output formatting rules]
- - Issue list: show title, short_id, status, first_seen, last_seen, count, environments, top_tags; order by most recent.
- - Event detail: include culprit, timestamp, environment, release, url.
- - If no results, state explicitly.
- - Redact PII in output (emails, IPs). Do not print raw stack traces.
- - Never echo auth tokens.

### 適用場景
- 適合在需要「"Use when the user asks to inspect Sentry issues or events, summarize recent pr…」的工作階段使用。
- 常見觸發語句：Use when the user asks to inspect Sentry issues or events, summarize recent production errors, or pull basic Sentry health data via the Sentry API; perform read-only queries with the bundled script and require `SENTRY_AUTH_TOKEN`.

### 原文精華摘錄
> "Use when the user asks to inspect Sentry issues or events, summarize recent production errors, or pull basic Sentry health data via the Sentry API
> perform read-only queries with the bundled script and require `SENTRY_AUTH_TOKEN`."
> Quick start
> - If not already authenticated, ask the user to provide a valid `SENTRY_AUTH_TOKEN` (read-only scopes such as `project:read`, `event:read`) or to log in and create one before running commands.
> - Set `SENTRY_AUTH_TOKEN` as an env var.
> - Optional defaults: `SENTRY_ORG`, `SENTRY_PROJECT`, `SENTRY_BASE_URL`.
> - Defaults: org/project `{your-org}`/`{your-project}`, time range `24h`, environment `prod`, limit 20 (max 50).
> Core tasks (use bundled script)
> Use `scripts/sentry_api.py` for deterministic API calls. It handles pagination and retries once on transient errors.
> Skill path (set once)

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：api, data, formatting, output
- analytics-tracking（marketingskills） - 相似 - 共享領域：meta, planning, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：asks, core, events, output
- interview-script（pm-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：core, script, test
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：api, asks, defaults, quick

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
