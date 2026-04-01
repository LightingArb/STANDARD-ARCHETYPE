---
## superpowers / dispatching-parallel-agents

### 來源
- repo：superpowers
- 路徑：skills/dispatching-parallel-agents/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/dispatching-parallel-agents/skill.md

### 一句話定位
把彼此獨立的任務拆給多個 agent 並行處理的調度技能。

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 協作導向, 拆解導向

### 核心思考框架
- [Overview]
- **Core principle:** Dispatch one agent per independent problem domain. Let them work concurrently.
- [The Pattern]
- Group failures by what's broken:
- - File A tests: Tool approval flow
- - File B tests: Batch completion behavior
- - File C tests: Abort functionality
- Each agent gets:
- - **Specific scope:** One test file or subsystem

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- **Don't use when:**
- - **Constraints:** Don't change other code
- - Verify fixes don't conflict
- **❌ Vague output:** "Fix it" - you don't know what changed
- **Exploratory debugging:** You don't know what's broken yet
- 3. **Independence** - Agents don't interfere with each other

### 提問方式
- 3. **Specific about output** - What should the agent return?
- 2. **Check for conflicts** - Did agents edit same code?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「把彼此獨立的任務拆給多個 agent 並行處理的調度技能」的工作階段使用。

### 原文精華摘錄
> Use when facing 2+ independent tasks that can be worked on without shared state or sequential dependencies
> Overview
> **Core principle:** Dispatch one agent per independent problem domain. Let them work concurrently.
> When to Use
> **Use when:**
> - 3+ test files failing with different root causes
> - Multiple subsystems broken independently
> - Each problem can be understood without context from others
> The Pattern
> Group failures by what's broken:

### 和其他 skill 的潛在關聯
- deepinit（oh-my-claudecode） - 相似 - 共享領域：meta, parallel；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：dependencies, example, key
- team（oh-my-claudecode） - 相似 - 共享領域：meta, parallel；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：agent, shared, state
- sciomc（oh-my-claudecode） - 相似 - 共享領域：meta, parallel；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：key, overview
- claude-api（anthropics-skills） - 互補 - 共享領域：meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：agent, common, prompt, tasks

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
