---
## openai-skills / yeet

### 來源
- repo：openai-skills
- 路徑：skills/.curated/yeet/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/yeet/skill.md

### 一句話定位
"Use only when the user explicitly asks to stage, commit, push, and open a GitH…

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
- [Workflow]
- - If on main/master/default, create a branch: `git checkout -b "codex/{description}"`
- - Otherwise stay on the current branch.
- - Confirm status, then stage everything: `git status -sb` then `git add -A`.
- - Commit tersely with the description: `git commit -m "{description}"`
- - Run checks if not already. If checks fail due to missing deps/tools, install dependencies and rerun once.
- - Push with tracking: `git push -u origin $(git branch --show-current)`

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use only when the user explicitly asks to stage, commit, push, and open a GitH…」的工作階段使用。
- 常見觸發語句：Use only when the user explicitly asks to stage, commit, push, and open a GitHub pull request in one flow using the GitHub CLI (`gh`).

### 原文精華摘錄
> "Use only when the user explicitly asks to stage, commit, push, and open a GitHub pull request in one flow using the GitHub CLI (`gh`)."
> Prerequisites
> - Require GitHub CLI `gh`. Check `gh --version`. If missing, ask the user to install `gh` and stop.
> - Require authenticated `gh` session. Run `gh auth status`. If not authenticated, ask the user to run `gh auth login` (and re-run `gh auth status`) before continuing.
> Naming conventions
> - Branch: `codex/{description}` when starting from main/master/default.
> - Commit: `{description}` (terse).
> - PR title: `[codex] {description}` summarizing the full diff.
> Workflow
> - If on main/master/default, create a branch: `git checkout -b "codex/{description}"`

### 和其他 skill 的潛在關聯
- pdf（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：asks, one
- CLAUDE（superpowers） - 相似 - 共享領域：design, meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：pull, request
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：asks
- doc-coauthoring（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：stage

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
