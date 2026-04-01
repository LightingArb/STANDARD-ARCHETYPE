---
## openai-skills / render-deploy

### 來源
- repo：openai-skills
- 路徑：skills/.curated/render-deploy/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/render-deploy/skill.md

### 一句話定位
Deploy applications to Render by analyzing codebases, generating render.yaml Bl…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Blueprint Workflow]
- **Key Points:**
- - Always use `plan: free` unless user specifies otherwise
- - Include ALL environment variables the app needs
- - Mark secrets with `sync: false` (user fills these in Dashboard)
- - Use appropriate service type: `web`, `worker`, `cron`, `static`, or `pserv`
- - Use appropriate runtime: [references/runtimes.md](references/runtimes.md)
- [Direct Creation Workflow]
- 1. Confirm the latest deploy is `live` and serving traffic
- 2. Hit the health endpoint (or root) and verify a 200 response
- 3. Scan recent error logs for a clear failure signature
- 4. Verify required env vars and port binding (`0.0.0.0:$PORT`)
- If the service fails to start or health checks time out, use the basic triage guide:

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - When you don't need a render.yaml file in your repo
- - Use the Render Dashboard/API to deploy a prebuilt Docker image (MCP cannot create image-backed services).
- **CRITICAL:** Ensure the user has merged and pushed the render.yaml file to their repository before clicking the deeplink. If the file isn't in the repository, Render cannot read the Blueprint configuration and deployment will fail.

### 提問方式
無明確提問模板

### 審查維度
- Deploy applications to Render by analyzing codebases, generating render.yaml Blueprints, and providing Dashboard deeplinks

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Deploy applications to Render by analyzing codebases, generating render.yaml Bl…」的工作階段使用。
- 常出現在收尾、發布、分支整理或部署交接階段。

### 原文精華摘錄
> Deploy applications to Render by analyzing codebases, generating render.yaml Blueprints, and providing Dashboard deeplinks
> Use when the user wants to deploy, host, publish, or set up their application on Render's cloud platform
> Prerequisites
> - When sandboxing blocks the deployment network calls, rerun with `sandbox_permissions=require_escalated`.
> - The deployment might take a few minutes. Use appropriate timeout values.
> When to Use This Skill
> Activate this skill when users want to:
> - Deploy an application to Render
> - Create a render.yaml Blueprint file
> - Set up Render deployment for their project

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：analyzing, new, path, wants
- ad-creative（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：generating, new, platform, wants
- churn-prevention（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：set, users, wants
- customer-research（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：analyzing, new, wants

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
