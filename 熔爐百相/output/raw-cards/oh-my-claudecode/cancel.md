---
## oh-my-claudecode / cancel

### 來源
- repo：oh-my-claudecode
- 路徑：skills/cancel/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/cancel/skill.md

### 一句話定位
Cancel any active OMC mode (autopilot, ralph, ultrawork, ultraqa, swarm, ultrap…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- any state tool, you MUST first load all of them via `ToolSearch`:
- `state_get_status`) may be registered as **deferred tools** by Claude Code. Before calling

禁止做
- cleanup that cannot be done via file deletion alone.

### 提問方式
無明確提問模板

### 審查維度
- Cancel any active OMC mode (autopilot, ralph, ultrawork, ultraqa, swarm, ultrapilot, pipeline, team)

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Cancel any active OMC mode (autopilot, ralph, ultrawork, ultraqa, swarm, ultrap…」的工作階段使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Cancel any active OMC mode (autopilot, ralph, ultrawork, ultraqa, swarm, ultrapilot, pipeline, team)
> What It Does
> Automatically detects which mode is active and cancels it:
> - **Autopilot**: Stops workflow, preserves progress for resume
> - **Ralph**: Stops persistence loop, clears linked ultrawork if applicable
> - **Ultrawork**: Stops parallel execution (standalone or linked)
> Usage
> Or say: "cancelomc", "stopomc"
> Critical: Deferred Tool Handling
> any state tool, you MUST first load all of them via `ToolSearch`:

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：meta, review, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：all, critical, pipeline
- figma-use（openai-skills） - 相似 - 共享領域：meta, review, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：critical, reference, tool
- pptx（anthropics-skills） - 相似 - 共享領域：meta, review, shipping；共享分類：審查型、流程型、元技能型；共同關鍵詞：notes, reference, what
- design-html（gstack） - 相似 - 共享領域：meta, review, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：steps, what

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
