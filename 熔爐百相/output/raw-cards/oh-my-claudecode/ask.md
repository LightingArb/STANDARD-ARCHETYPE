---
## oh-my-claudecode / ask

### 來源
- repo：oh-my-claudecode
- 路徑：skills/ask/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/ask/skill.md

### 一句話定位
Process-first advisor routing for Claude, Codex, or Gemini via omc ask, with ar…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- **Do NOT manually construct raw provider CLI commands.** Never run `codex`, `claude`, or `gemini` directly to fulfill this skill. The `omc ask` wrapper handles correct flag selection, artifact persistence, and provider-version compatibility automatically. Manually assembling provider CLI flags will produce incorrect or outdated invocations.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Process-first advisor routing for Claude, Codex, or Gemini via omc ask, with ar…」的工作階段使用。

### 原文精華摘錄
> Process-first advisor routing for Claude, Codex, or Gemini via `omc ask`, with artifact capture and no raw CLI assembly
> Usage
> Examples:
> Routing
> **Required execution path — always use this command:**
> **Do NOT manually construct raw provider CLI commands.** Never run `codex`, `claude`, or `gemini` directly to fulfill this skill. The `omc ask` wrapper handles correct flag selection, artifact persistence, and provider-version compatibility automatically. M...
> Requirements
> - The selected local CLI must be installed and authenticated.
> - Verify availability with the matching command:
> Artifacts

### 和其他 skill 的潛在關聯
- security-ownership-map（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：artifacts, requirements
- sentry（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：requirements, via
- sora（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：cli, via
- speech（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：cli, via

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
