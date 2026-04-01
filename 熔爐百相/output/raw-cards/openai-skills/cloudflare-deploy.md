---
## openai-skills / cloudflare-deploy

### 來源
- repo：openai-skills
- 路徑：skills/.curated/cloudflare-deploy/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/cloudflare-deploy/skill.md

### 一句話定位
Deploy applications and infrastructure to Cloudflare using Workers, Pages, and…

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
- [Quick Decision Trees]
- ### "I need to run code"
- ### "I need to store data"
- ### "I need AI/ML"
- ### "I need networking/connectivity"
- ### "I need security"
- ### "I need media/content"

### 核心行為規則
必須做
- ## Authentication (Required Before Deploy)

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Deploy applications and infrastructure to Cloudflare using Workers, Pages, and…」的工作階段使用。
- 常出現在收尾、發布、分支整理或部署交接階段。

### 原文精華摘錄
> Deploy applications and infrastructure to Cloudflare using Workers, Pages, and related platform services
> Use when the user asks to deploy, host, publish, or set up a project on Cloudflare
> Prerequisites
> - When sandboxing blocks the deployment network calls, rerun with `sandbox_permissions=require_escalated`.
> - The deployment might take a few minutes. Use appropriate timeout values.
> Authentication (Required Before Deploy)
> Verify auth before `wrangler deploy`, `wrangler pages deploy`, or `npm run deploy`:
> - Interactive/local: `wrangler login` (one-time OAuth)
> - CI/CD: Set `CLOUDFLARE_API_TOKEN` env var
> Quick Decision Trees

### 和其他 skill 的潛在關聯
- schema-markup（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：pages, product, quick, related
- pdf（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：asks, pages, quick
- copywriting（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：pages, product, related
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：prerequisites, product, quick

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
