---
## oh-my-claudecode / learner

### 來源
- repo：oh-my-claudecode
- 路徑：skills/learner/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/learner/skill.md

### 一句話定位
Extract a learned skill from the current conversation

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 反思性

### 核心思考框架
- [Workflow]
- - **Problem Statement**: The SPECIFIC error, symptom, or confusion that occurred
- - Include actual error messages, file paths, line numbers
- - Example: "TypeError in src/hooks/session.ts:45 when sessionId is undefined after restart"
- - **Solution**: The EXACT fix, not general advice
- - Include code snippets, file paths, configuration changes
- - Example: "Add null check before accessing session.user, regenerate session on 401"
- [Recognition Pattern]
- How do you know when this skill applies? What are the signs?

### 核心行為規則
必須做
- Before extracting a skill, ALL three must be true:
- Before saving, determine if the learning is:

禁止做
- ### Anti-Patterns (DO NOT EXTRACT)
- What goes wrong if you don't know this? What symptom led you here?

### 提問方式
- What goes wrong if you don't know this? What symptom led you here?
- How do you know when this skill applies? What are the signs?
- The decision-making heuristic, not just code. How should Claude THINK about this?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Extract a learned skill from the current conversation」的工作階段使用。

### 原文精華摘錄
> Extract a learned skill from the current conversation
> Expertise
> **The difference:**
> - BAD (mimicking): "When you see ConnectionResetError, add this try/except block"
> - GOOD (reusable skill): "In async network code, any I/O operation can fail independently due to client/server lifecycle mismatches. The principle: wrap each I/O operation separately, because failure between operations is the common case, not the exception."
> Before extracting a skill, ALL three must be true:
> Workflow
> - **Problem Statement**: The SPECIFIC error, symptom, or confusion that occurred
> - Include actual error messages, file paths, line numbers
> - Example: "TypeError in src/hooks/session.ts:45 when sessionId is undefined after restart"

### 和其他 skill 的潛在關聯
- security-ownership-map（openai-skills） - 相似 - 共享領域：learning, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：example
- webapp-testing（anthropics-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：approach, example, pattern
- design-html（gstack） - 相似 - 共享領域：learning, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型
- README（ui-ux-pro-max-skill） - 相似 - 共享領域：learning, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
