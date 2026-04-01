---
## oh-my-claudecode / setup

### 來源
- repo：oh-my-claudecode
- 路徑：skills/setup/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/setup/skill.md

### 一句話定位
Use first for install/update routing — sends setup, doctor, or MCP requests to…

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

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
- 適合在需要「Use first for install/update routing — sends setup, doctor, or MCP requests to…」的工作階段使用。
- 常出現在正式施工前的環境準備與工具接線階段。

### 原文精華摘錄
> Use first for install/update routing — sends setup, doctor, or MCP requests to the correct OMC setup flow
> Usage
> Routing
> Process the request by the **first argument only** so install/setup questions land on the right flow immediately:
> - No argument, `wizard`, `local`, `global`, or `--force` -> route to `/oh-my-claudecode:omc-setup` with the same remaining args
> - `doctor` -> route to `/oh-my-claudecode:omc-doctor` with everything after the `doctor` token
> - `mcp` -> route to `/oh-my-claudecode:mcp-setup` with everything after the `mcp` token
> Notes
> - `/oh-my-claudecode:omc-setup`, `/oh-my-claudecode:omc-doctor`, and `/oh-my-claudecode:mcp-setup` remain valid compatibility entrypoints.
> - Prefer `/oh-my-claudecode:setup` in new documentation and user guidance.

### 和其他 skill 的潛在關聯
- design-review（gstack） - 互補 - 共享分類：流程型；共同關鍵詞：first, flow, routing, setup
- gstack-upgrade（gstack） - 互補 - 共享領域：meta；共享分類：工具程序型、流程型；共同關鍵詞：flow, install, update, usage
- SKILL（gstack） - 互補 - 共享分類：工具程序型、流程型；共同關鍵詞：first, flow, routing, setup
- design-html（gstack） - 互補 - 共享領域：meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：first, routing, setup

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
