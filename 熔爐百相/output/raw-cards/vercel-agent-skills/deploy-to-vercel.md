---
## vercel-agent-skills / deploy-to-vercel

### 來源
- repo：vercel-agent-skills
- 路徑：skills/deploy-to-vercel/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：vercel-agent-skills::skills/deploy-to-vercel/skill.md

### 一句話定位
Deploy applications and websites to Vercel.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- 1. **Ask the user before pushing.** Never push without explicit approval:
- 1. **Ask the user which team to deploy to.** Present the team slugs from Step 1 as a bulleted list. If there's only one team (or just a personal account), skip this step.

禁止做
- 1. **Ask the user before pushing.** Never push without explicit approval:
- **Do not** curl or fetch the deployed URL to verify it works. Just return the link.
- If deployment fails due to network issues (timeouts, DNS errors, connection resets) in a sandboxed environment, rerun with escalated permissions (`sandbox_permissions=require_escalated`). Do not escalate the initial CLI availability check — only escalate the actual deploy command.
- 1. **Ask the user which team to deploy to.** Present the team slugs from Step 1 as a bulleted list. If there's only one team (or just a personal account), skip this step.
- You likely cannot run `vercel login` or `git push`. Go directly to the **no-auth fallback — claude.ai sandbox**.

### 提問方式
無明確提問模板

### 審查維度
- Use when the user requests deployment actions like "deploy my app", "deploy and give me the link", "push this live", or "create a preview deployment"

### 輸出格式要求
- [Output]
- - **Git push:** Use `vercel ls --format json` to find the preview URL. If the CLI isn't authenticated, tell the user to check the Vercel dashboard or commit status checks.
- - **CLI deploy:** Show the URL returned by `vercel deploy --no-wait`. Use `vercel inspect <url>` to check build status and report it to the user.
- - **No-auth fallback:** Show both the preview URL and the claim URL:
- **Do not** curl or fetch the deployed URL to verify it works. Just return the link.

### 適用場景
- 適合在需要「Deploy applications and websites to Vercel.」的工作階段使用。
- 常見觸發語句：deploy my app / deploy and give me the link / push this live / create a preview deployment
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Deploy applications and websites to Vercel
> Use when the user requests deployment actions like "deploy my app", "deploy and give me the link", "push this live", or "create a preview deployment"
> Step 1: Gather Project State
> Run all four checks before deciding which method to use:
> Pass the team slug via `--scope` on all subsequent CLI commands (`vercel deploy`, `vercel link`, `vercel inspect`, etc.):
> **About the `.vercel/` directory:** A linked project has either:
> - `.vercel/project.json` — created by `vercel link` (single project linking). Contains `projectId` and `orgId`.
> Step 2: Choose a Deploy Method
> 1. **Ask the user before pushing.** Never push without explicit approval:
> 2. **Commit and push:**

### 和其他 skill 的潛在關聯
- vercel-deploy（openai-skills） - 相似 - 共享領域：meta, review, shipping；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：actions, app, applications, deploy
- netlify-deploy（openai-skills） - 相似 - 共享領域：meta, review, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：deploy, link, preview, troubleshooting
- winui-app（openai-skills） - 相似 - 共享領域：meta, review, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：app, applications, deployment, troubleshooting
- design-html（gstack） - 相似 - 共享領域：meta, review, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：live, preview, step

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
