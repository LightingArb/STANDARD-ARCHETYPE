---
## openai-skills / security-threat-model

### 來源
- repo：openai-skills
- 路徑：skills/.curated/security-threat-model/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/security-threat-model/skill.md

### 一句話定位
"Repository-grounded threat modeling that enumerates trust boundaries, assets,…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 品味導向

### 核心思考框架
- [Workflow]
- - Identify primary components, data stores, and external integrations from the repo summary.
- - Identify how the system runs (server, CLI, library, worker) and its entrypoints.
- - Separate runtime behavior from CI/build/dev tooling and from tests/examples.
- - Map the in-scope locations to those components and exclude out-of-scope items explicitly.
- - Do not claim components, flows, or controls without evidence.
- - Enumerate trust boundaries as concrete edges between components, noting protocol, auth, encryption, validation, and rate limiting.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Do not claim components, flows, or controls without evidence.

### 提問方式
無明確提問模板

### 審查維度
- Do not trigger for general architecture summaries, code review, or non-security design work."

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Repository-grounded threat modeling that enumerates trust boundaries, assets,…」的工作階段使用。
- 常見觸發語句：Repository-grounded threat modeling that enumerates trust boundaries, assets, attacker capabilities, abuse paths, and mitigations, and writes a concise Markdown threat model. Trigger only when the user explicitly asks to threat model a codebase or path, enumerate threats/abuse paths, or perform AppSec threat modeling. Do not trigger for general architecture summaries, code review, or non-security design work.
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> "Repository-grounded threat modeling that enumerates trust boundaries, assets, attacker capabilities, abuse paths, and mitigations, and writes a concise Markdown threat model
> Trigger only when the user explicitly asks to threat model a codebase or path, enumerate threats/abuse paths, or perform AppSec threat modeling
> Do not trigger for general architecture summaries, code review, or non-security design work."
> Quick start
> 1) Collect (or infer) inputs:
> - Repo root path and any in-scope paths.
> - Intended usage, deployment model, internet exposure, and auth expectations (if known).
> - Any existing repository summary or architecture spec.
> Workflow
> - Identify primary components, data stores, and external integrations from the repo summary.

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：path, references, trigger
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、流程型、元技能型；共同關鍵詞：quick, references, trigger
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：architecture, asks, general, quick
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：quick

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
