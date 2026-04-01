---
## openai-skills / gh-fix-ci

### 來源
- repo：openai-skills
- 路徑：skills/.curated/gh-fix-ci/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/gh-fix-ci/skill.md

### 一句話定位
"Use when a user asks to debug or fix failing GitHub PR checks that run in GitH…

### 核心人格特質
流程紀律, 完成導向, 規範導向, 元認知, 證據導向, 懷疑論, 挑戰性, 野心導向, 產品導向

### 核心思考框架
- [Overview]
- - If a plan-oriented skill (for example `create-plan`) is available, use it; otherwise draft a concise plan inline and request approval before implementing.
- [Workflow]
- 1. Verify gh authentication.
- - Run `gh auth status` in the repo.
- - If unauthenticated, ask the user to run `gh auth login` (ensuring repo + workflow scopes) before proceeding.
- 2. Resolve the PR.
- - Prefer the current branch PR: `gh pr view --json number,url`.
- - If the user provides a PR number or URL, use that directly.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Do not attempt Buildkite or other providers; keep the workflow lean.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use when a user asks to debug or fix failing GitHub PR checks that run in GitH…」的工作階段使用。
- 常見觸發語句：Use when a user asks to debug or fix failing GitHub PR checks that run in GitHub Actions; use `gh` to inspect checks and logs, summarize failure context, draft a fix plan, and implement only after explicit approval. Treat external providers (for example Buildkite) as out of scope and report only the details URL.
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> "Use when a user asks to debug or fix failing GitHub PR checks that run in GitHub Actions
> use `gh` to inspect checks and logs, summarize failure context, draft a fix plan, and implement only after explicit approval
> Treat external providers (for example Buildkite) as out of scope and report only the details URL."
> Overview
> - If a plan-oriented skill (for example `create-plan`) is available, use it; otherwise draft a concise plan inline and request approval before implementing.
> Inputs
> - `repo`: path inside the repo (default `.`)
> - `pr`: PR number or URL (optional; defaults to current branch PR)
> - `gh` authentication for the repo host
> Quick start

### 和其他 skill 的潛在關聯
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：流程型、元技能型；共同關鍵詞：actions, example, fix, implement
- ab-test-setup（marketingskills） - 相似 - 共享領域：debugging, design, meta；共享分類：流程型、元技能型；共同關鍵詞：implement, plan
- design-html（gstack） - 相似 - 共享領域：design, meta, planning；共享分類：流程型、元技能型；共同關鍵詞：after, implement, plan, report
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：流程型、元技能型；共同關鍵詞：fix, overview, report

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
