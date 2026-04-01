---
## oh-my-claudecode / AGENTS

### 來源
- repo：oh-my-claudecode
- 路徑：src/hooks/AGENTS.md
- 檔案類型：AGENTS.md
- card_kind：overview
- language：en
- canonical_group：oh-my-claudecode::src/hooks/agents.md

### 一句話定位
31 event-driven hooks that power execution modes and behaviors.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- // Stop hook ALWAYS returns continue: true
- // UserPromptSubmit - Before prompt is sent
- // Stop - Before session ends
- // PreToolUse - Before tool execution
- | `UserPromptSubmit` | Before prompt processing | Keyword detection, mode activation |
- | `Stop` | Before session ends | Continuation enforcement |
- | `PreToolUse` | Before tool execution | Permission validation |

禁止做
- | `ralph/` | Persistence until verified | "ralph", "don't stop" |

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「31 event-driven hooks that power execution modes and behaviors.」的工作階段使用。

### 原文精華摘錄
> 31 event-driven hooks that power execution modes and behaviors
> Purpose
> Hooks intercept Claude Code events to enable:
> - **Execution modes**: autopilot, ultrawork, ralph, ultrapilot, swarm, pipeline (mode-registry)
> - **Validation**: thinking blocks, empty messages, comments
> - **Recovery**: edit errors, session recovery, context window
> Key Files
> | File | Description |
> |------|-------------|
> | `index.ts` | Re-exports all hooks |

### 和其他 skill 的潛在關聯
- skill-creator（anthropics-skills） - 相似 - 共享領域：meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：files, key
- figma-generate-library（openai-skills） - 相似 - 共享領域：meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：modes, state
- draft-nda（pm-skills） - 相似 - 共享領域：meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：key, purpose
- privacy-policy（pm-skills） - 相似 - 共享領域：meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：key, purpose

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
