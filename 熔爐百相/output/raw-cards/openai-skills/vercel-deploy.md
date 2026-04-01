---
## openai-skills / vercel-deploy

### 來源
- repo：openai-skills
- 路徑：skills/.curated/vercel-deploy/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/vercel-deploy/skill.md

### 一句話定位
Deploy applications and websites to Vercel.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- **Do not** curl or fetch the deployed URL to verify it works. Just return the link.
- If deployment fails due to network issues (timeouts, DNS errors, connection resets), rerun the actual deploy command with escalated permissions (use `sandbox_permissions=require_escalated`). Do not escalate the `command -v vercel` installation check. The deploy requires escalated network access when sandbox networking blocks outbound requests.

### 提問方式
無明確提問模板

### 審查維度
- Use when the user requests deployment actions like "deploy my app", "deploy and give me the link", "push this live", or "create a preview deployment"

### 輸出格式要求
- [Output]
- **Do not** curl or fetch the deployed URL to verify it works. Just return the link.

### 適用場景
- 適合在需要「Deploy applications and websites to Vercel.」的工作階段使用。
- 常見觸發語句：deploy my app / deploy and give me the link / push this live / create a preview deployment
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Deploy applications and websites to Vercel
> Use when the user requests deployment actions like "deploy my app", "deploy and give me the link", "push this live", or "create a preview deployment"
> Prerequisites
> - Check whether the Vercel CLI is installed **without** escalated permissions (for example, `command -v vercel`).
> - Only escalate the actual deploy command if sandboxing blocks the deployment network calls (`sandbox_permissions=require_escalated`).
> - The deployment might take a few minutes. Use appropriate timeout values.
> Quick Start
> 1. Check whether the Vercel CLI is installed (no escalation for this check):
> 2. If `vercel` is installed, run this (with a 10 minute timeout):
> **Important:** Use a 10 minute (600000ms) timeout for the deploy command since builds can take a while.

### 和其他 skill 的潛在關聯
- deploy-to-vercel（vercel-agent-skills） - 相似 - 共享領域：meta, review, shipping；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：actions, app, applications, deploy
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：actions, app, output, prerequisites
- seo-audit（marketingskills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：like, output, start
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, review；共享分類：審查型、元技能型；共同關鍵詞：like, output, quick

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [ ] 流程型
- [x] 元技能型
---
