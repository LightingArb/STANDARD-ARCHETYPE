---
## openai-skills / netlify-deploy

### 來源
- repo：openai-skills
- 路徑：skills/.curated/netlify-deploy/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/netlify-deploy/skill.md

### 一句話定位
Deploy web projects to Netlify using the Netlify CLI (npx netlify).

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Overview]
- This skill automates Netlify deployments by:
- - Verifying Netlify CLI authentication
- - Detecting project configuration and framework
- - Linking to existing sites or creating new ones
- - Deploying to production or preview environments
- [Authentication Pattern]
- The skill uses the **pre-authenticated Netlify CLI** approach:
- 1. Check authentication status with `npx netlify status`
- 2. If not authenticated, guide user through `npx netlify login`
- 3. Fail gracefully if authentication cannot be established
- Authentication uses either:
- - **Browser-based OAuth** (primary): `netlify login` opens browser for authentication
- [Workflow]
- Check if the user is logged into Netlify:
- **Expected output patterns**:
- - ✅ Authenticated: Shows logged-in user email and site link status
- - ❌ Not authenticated: "Not logged into any site" or authentication error

### 核心行為規則
必須做
- Before deploying, ensure project dependencies are installed:

禁止做
- 1. Never commit secrets to Git
- 3. Fail gracefully if authentication cannot be established

### 提問方式
無明確提問模板

### 審查維度
- Use when the user asks to deploy, host, publish, or link a site/repo on Netlify, including preview and production deploys

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Deploy web projects to Netlify using the Netlify CLI (npx netlify).」的工作階段使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Deploy web projects to Netlify using the Netlify CLI (`npx netlify`)
> Use when the user asks to deploy, host, publish, or link a site/repo on Netlify, including preview and production deploys
> Overview
> This skill automates Netlify deployments by:
> - Verifying Netlify CLI authentication
> - Detecting project configuration and framework
> - Linking to existing sites or creating new ones
> Prerequisites
> - **Netlify CLI**: Installed via npx (no global install required)
> - **Authentication**: Netlify account with active login session

### 和其他 skill 的潛在關聯
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：example, prerequisites, projects, reference
- webapp-testing（anthropics-skills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：example, pattern, reference, web
- seo-audit（marketingskills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：references, site, web
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：overview, references

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
