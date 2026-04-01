---
## oh-my-claudecode / omc-setup

### 來源
- repo：oh-my-claudecode
- 路徑：skills/omc-setup/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/omc-setup/skill.md

### 一句話定位
Install or refresh oh-my-claudecode for plugin, npm, and local-dev setups from…

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
- [Phase Execution]
- ### For `--local` or `--global` flags:
- ### For full setup (default or --force):
- Execute phases sequentially. For each phase, read the corresponding file and follow its instructions:
- 1. **Phase 1 - Install CLAUDE.md**: Read `${CLAUDE_PLUGIN_ROOT}/skills/omc-setup/phases/01-install-claude-md.md` and follow its instructions.
- 2. **Phase 2 - Environment Configuration**: Read `${CLAUDE_PLUGIN_ROOT}/skills/omc-setup/phases/02-configure.md` and follow its instructions. Phase 2 must delegate HUD/statusLine setup to the `hud` skill; do not generate or patch `statusLine` paths inline here.
- 3. **Phase 3 - Integration Setup**: Read `${CLAUDE_PLUGIN_ROOT}/skills/omc-setup/phases/03-integrations.md` and follow its instructions.

### 核心行為規則
必須做
- Before starting any phase, check for existing state:
- **CRITICAL**: Before doing anything else, check if setup has already been completed. This prevents users from having to re-run the full setup wizard after every update.
- Use AskUserQuestion to prompt:

禁止做
- **When this skill is invoked, immediately execute the workflow below. Do not only restate or summarize these instructions back to the user.**
- If user passes `--force` flag, skip this check and proceed directly to setup.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Install or refresh oh-my-claudecode for plugin, npm, and local-dev setups from…」的工作階段使用。
- 常出現在正式施工前的環境準備與工具接線階段。

### 原文精華摘錄
> Install or refresh oh-my-claudecode for plugin, npm, and local-dev setups from the canonical setup flow
> Best-Fit Use
> - Marketplace/plugin install users should land here after `/plugin install oh-my-claudecode`
> - npm users should land here after `npm i -g oh-my-claude-sisyphus@latest`
> - local-dev and worktree users should land here after updating the checked-out repo and rerunning setup
> Flag Parsing
> Check for flags in the user's invocation:
> - `--help` → Show Help Text (below) and stop
> - `--local` → Phase 1 only (target=local), then stop
> - `--global` → Phase 1 only (target=global), then stop

### 和其他 skill 的潛在關聯
- design-html（gstack） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：detection, setup, text
- onboarding-cro（marketingskills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：flow, handling, setup
- copy-editing（marketingskills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：already, text
- copywriting（marketingskills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：help, text

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
