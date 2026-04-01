---
## oh-my-claudecode / ralplan

### 來源
- repo：oh-my-claudecode
- 路徑：skills/ralplan/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/ralplan/skill.md

### 一句話定位
Consensus planning entrypoint that auto-gates vague ralph/autopilot/team reques…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- 5. **Re-review loop** (max 5 iterations): Any non-`APPROVE` Critic verdict (`ITERATE` or `REJECT`) MUST run the same full closed loop:
- > **Important:** Steps 3 and 4 MUST run sequentially. Do NOT issue both agent Task calls in the same parallel batch. Always await the Architect result before issuing the Critic Task.

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Consensus planning entrypoint that auto-gates vague ralph/autopilot/team reques…」的工作階段使用。
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> Consensus planning entrypoint that auto-gates vague ralph/autopilot/team requests before execution
> Usage
> Flags
> - `--interactive`: Enables user prompts at key decision points (draft review in step 2 and final approval in step 6). Without this flag the workflow runs fully automated — Planner → Architect → Critic loop — and outputs the final plan without asking for con...
> - `--deliberate`: Forces deliberate mode for high-risk work. Adds pre-mortem (3 scenarios) and expanded test planning (unit/integration/e2e/observability). Without this flag, deliberate mode can still auto-enable when the request explicitly signals high ris...
> - `--architect codex`: Use Codex for the Architect pass when Codex CLI is available. Otherwise, briefly note the fallback and keep the default Claude Architect review.
> - `--critic codex`: Use Codex for the Critic pass when Codex CLI is available. Otherwise, briefly note the fallback and keep the default Claude Critic review.
> Usage with interactive mode
> Behavior
> This skill invokes the Plan skill in consensus mode:

### 和其他 skill 的潛在關聯
- free-tool-strategy（marketingskills） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：interactive
- lead-magnets（marketingskills） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：interactive
- marketing-ideas（marketingskills） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：execution
- paywall-upgrade-cro（marketingskills） - 相似 - 共享領域：meta, planning, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：gate

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
