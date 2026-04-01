---
## marketingskills / revops

### 來源
- repo：marketingskills
- 路徑：skills/revops/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：marketingskills::skills/revops/skill.md

### 一句話定位
"When the user wants help with revenue operations, lead lifecycle management, o…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Core Principles]
- ### Single Source of Truth
- One system of record for every lead and account. If data lives in multiple places, it will conflict. Pick a CRM as the canonical source and sync everything to it.
- ### Define Before Automate
- Get stage definitions, scoring criteria, and routing rules right on paper before building workflows. Automating a broken process just creates broken results faster.
- ### Measure Every Handoff
- Every handoff between teams is a potential leak. Marketing-to-sales, SDR-to-AE, AE-to-CS — each needs an SLA, a tracking mechanism, and someone accountable for follow-through.
- [CRM Automation Workflows]
- - **Lifecycle stage updates** — Auto-advance stages when criteria are met
- - **Task creation on handoff** — Create follow-up task when MQL assigned to rep
- - **SLA alerts** — Notify manager if rep misses response time SLA
- - **Deal stage triggers** — Auto-send proposals, update forecasts, notify CS on close
- - **MQL alert** — Instant notification to assigned rep with lead context
- - **Meeting booked** — Notify AE when prospect books via scheduling tool
- [Deal Desk Processes]
- - ACV above **$25K** (or your threshold for non-standard deals)
- - Non-standard payment terms (net-90, quarterly billing)
- - Multi-year contracts with custom pricing

### 核心行為規則
必須做
- ## Before Starting
- ### Define Before Automate

禁止做
- Work with whatever the user gives you. If they have a clear problem area, start there. Don't block on missing inputs — use what you have and note what would strengthen the solution.
- - **Required fields per stage** — Don't let reps advance a deal without filling in required data
- - Use progressive profiling — don't require everything upfront

### 提問方式
- 1. **GTM motion** — Product-led (PLG), sales-led, or hybrid?
- 2. **ACV range** — What's the average contract value?
- 3. **Sales cycle length** — Days from first touch to closed-won?
- 4. **Current stack** — CRM, marketing automation, scheduling, enrichment tools?
- 5. **Current state** — How are leads managed today? What's working and what's not?
- 6. **Goals** — Increase conversion? Reduce speed-to-lead? Fix handoff leaks? Build from scratch?
- 5. Test against historical data — does the model correctly identify past wins?
- [Task-Specific Questions]
- 1. What CRM platform are you using (or planning to use)?
- 2. How many leads per month do you generate?
- 3. What's your current MQL definition?
- 4. Where do leads get stuck in your funnel?

### 審查維度
非審查型

### 輸出格式要求
- [RevOps Metrics Dashboard]
- Build three views:
- 1. **Marketing view** — Lead volume, MQL rate, source attribution, cost per MQL
- 2. **Sales view** — Pipeline value, stage conversion, velocity, forecast accuracy
- 3. **Executive view** — CAC, LTV:CAC, revenue vs. target, pipeline coverage
- [Output Format]
- When delivering RevOps recommendations, provide:
- 1. **Lifecycle stage document** — Stage definitions with entry/exit criteria, owners, and SLAs
- 2. **Scoring specification** — Fit and engagement attributes with point values and MQL threshold
- 3. **Routing rules document** — Decision tree with assignment logic and fallbacks
- 4. **Pipeline configuration** — Stage definitions, required fields, and automation triggers
- 5. **Metrics dashboard spec** — Key metrics, data sources, and target benchmarks

### 適用場景
- 適合在需要「"When the user wants help with revenue operations, lead lifecycle management, o…」的工作階段使用。
- 常見觸發語句：When the user wants help with revenue operations, lead lifecycle management, or marketing-to-sales handoff processes. Also use when the user mentions 'RevOps,' 'revenue operations,' 'lead scoring,' 'lead routing,' 'MQL,' 'SQL,' 'pipeline stages,' 'deal desk,' 'CRM automation,' 'marketing-to-sales handoff,' 'data hygiene,' 'leads aren't getting to sales,' 'pipeline management,' 'lead qualification,' or 'when should marketing hand off to sales.' Use this for anything involving the systems and processes that connect marketing to revenue. For cold outreach emails, see cold-email. For email drip campaigns, see email-sequence. For pricing decisions, see pricing-strategy.
- 常出現在正式施工前的環境準備與工具接線階段。

### 原文精華摘錄
> "When the user wants help with revenue operations, lead lifecycle management, or marketing-to-sales handoff processes
> Also use when the user mentions 'RevOps,' 'revenue operations,' 'lead scoring,' 'lead routing,' 'MQL,' 'SQL,' 'pipeline stages,' 'deal desk,' 'CRM automation,' 'marketing-to-sales handoff,' 'data hygiene,' 'leads aren...
> For cold outreach emails, see cold-email
> For email drip campaigns, see email-sequence
> For pricing decisions, see pricing-strategy."
> Before Starting
> **Check for product marketing context first:**
> Gather this context (ask if not provided):
> 1. **GTM motion** — Product-led (PLG), sales-led, or hybrid?
> 2. **ACV range** — What's the average contract value?

### 和其他 skill 的潛在關聯
- ship（gstack） - 相似 - 共享領域：shipping；共享分類：審查型、工具程序型、流程型；共同關鍵詞：dashboard, format, framework, metrics
- xlsx（anthropics-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：also, data, output, pipeline
- skill-creator（openai-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：core, integrations, principles, should
- autoplan（gstack） - 相似 - 共享領域：meta；共享分類：審查型、流程型、元技能型；共同關鍵詞：decisions, format, operations, pipeline

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
