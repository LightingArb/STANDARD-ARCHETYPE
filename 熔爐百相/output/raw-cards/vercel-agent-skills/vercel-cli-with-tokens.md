---
## vercel-agent-skills / vercel-cli-with-tokens

### 來源
- repo：vercel-agent-skills
- 路徑：skills/vercel-cli-with-tokens/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：vercel-agent-skills::skills/vercel-cli-with-tokens/skill.md

### 一句話定位
Deploy and manage projects on Vercel using token-based authentication.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- Before running any Vercel CLI commands, identify where the token is coming from. Work through these scenarios in order:
- 1. **Ask the user before pushing.** Never push without explicit approval.
- - Ask the user for a fresh token.

禁止做
- 1. **Ask the user before pushing.** Never push without explicit approval.
- - **Never pass `VERCEL_TOKEN` as a `--token` flag.** Export it as an environment variable and let the CLI read it natively.
- - **Ask before pushing to git.** Never push commits without the user's approval.
- - **Do not read or modify `.vercel/` files directly.** The CLI manages this directory.
- - **Do not curl/fetch deployed URLs to verify.** Just return the link to the user.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Deploy and manage projects on Vercel using token-based authentication.」的工作階段使用。
- 常見觸發語句：deploy to vercel / set up vercel / add environment variables to vercel
- 常出現在收尾、發布、分支整理或部署交接階段。

### 原文精華摘錄
> Deploy and manage projects on Vercel using token-based authentication
> Use when working with Vercel CLI using access tokens rather than interactive login — e.g
> "deploy to vercel", "set up vercel", "add environment variables to vercel"
> Step 1: Locate the Vercel Token
> Before running any Vercel CLI commands, identify where the token is coming from. Work through these scenarios in order:
> If found, export it:
> Look for any variable that looks like a Vercel token (Vercel tokens typically start with `vca_`):
> Inspect the output to identify which variable holds the token, then export it as `VERCEL_TOKEN`:
> Step 2: Locate the Project and Team
> **If you have a project URL** (e.g. `https://vercel.com/my-team/my-project`), extract the team slug:

### 和其他 skill 的潛在關聯
- netlify-deploy（openai-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：authentication, cli, deploy, environment
- cloudflare-deploy（openai-skills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：authentication, deploy, project, set
- figma-generate-library（openai-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：set, token, tokens, variables
- linear（openai-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：manage, projects, team, troubleshooting

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
