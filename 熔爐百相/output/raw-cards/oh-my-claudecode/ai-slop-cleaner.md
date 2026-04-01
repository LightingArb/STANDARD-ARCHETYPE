---
## oh-my-claudecode / ai-slop-cleaner

### 來源
- repo：oh-my-claudecode
- 路徑：skills/ai-slop-cleaner/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/ai-slop-cleaner/skill.md

### 一句話定位
Clean AI-generated code slop with a regression-safe, deletion-first workflow an…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Workflow]
- 1. **Protect current behavior first**
- - Identify what must stay the same.
- - Add or run the narrowest regression tests needed before editing.
- - If tests cannot come first, record the verification plan explicitly before touching code.
- 2. **Write a cleanup plan before code**
- - Bound the pass to the requested files or feature area.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- Do not use this skill when:
- - Do not silently expand a changed-file scope into broader cleanup work unless the user explicitly asks for it
- - Do not bundle unrelated refactors into the same edit set.
- - If tests cannot come first, record the verification plan explicitly before touching code.

### 提問方式
無明確提問模板

### 審查維度
- [Review Mode (`--review`)]
- - **Writer pass**: make the cleanup changes with behavior locked by tests.
- - **Reviewer pass**: inspect the cleanup plan, changed files, and verification evidence.
- - The same pass must not both write and self-approve high-impact cleanup without a separate review step.
- In review mode:
- 1. Do **not** start by editing files.
- 2. Review the cleanup plan, changed files, and regression coverage.

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Clean AI-generated code slop with a regression-safe, deletion-first workflow an…」的工作階段使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Clean AI-generated code slop with a regression-safe, deletion-first workflow and optional reviewer-only mode
> When to Use
> Use this skill when:
> - the user explicitly says `deslop`, `anti-slop`, or `AI slop`
> - the request is to clean up or refactor code that feels noisy, repetitive, or overly abstract
> - follow-up implementation left duplicate logic, dead code, wrapper layers, boundary leaks, or weak regression coverage
> When Not to Use
> Do not use this skill when:
> - the task is mainly a new feature build or product change
> - the user wants a broad redesign instead of an incremental cleanup pass

### 和其他 skill 的潛在關聯
- winui-app（openai-skills） - 相似 - 共享領域：meta, performance, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型
- xlsx（anthropics-skills） - 相似 - 共享領域：meta, planning, review；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：integration
- plan-eng-review（gstack） - 相似 - 共享領域：planning, review；共享分類：思考框架型、審查型、流程型；共同關鍵詞：execution, optional
- programmatic-seo（marketingskills） - 相似 - 共享領域：meta, planning, review；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：integration

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
