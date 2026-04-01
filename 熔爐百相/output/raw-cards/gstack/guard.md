---
## gstack / guard

### 來源
- repo：gstack
- 路徑：guard/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::guard/skill.md

### 一句話定位
同時啟用破壞性命令警告與目錄凍結的全量安全模式技能。

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 證據導向, 懷疑論, 挑戰性, 野心導向, 產品導向, 謹慎

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- Ask the user which directory to restrict edits to. Use AskUserQuestion:

禁止做
- - Question: "Guard mode: which directory should edits be restricted to? Destructive command warnings are always on. Files outside the chosen path will be blocked from editing."
- - "2. **Edit boundary** — file edits restricted to `<path>/`. Edits outside this directory are blocked."

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「同時啟用破壞性命令警告與目錄凍結的全量安全模式技能」的工作階段使用。
- 常見觸發語句：guard mode / full safety / lock it down / maximum safety
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> Full safety mode: destructive command warnings + directory-scoped edits
> Combines /careful (warns before rm -rf, DROP TABLE, force-push, etc.) with /freeze (blocks edits outside a specified directory)
> Use for maximum safety when touching prod or debugging live systems
> Use when asked to "guard mode", "full safety", "lock it down", or "maximum safety"
> (gstack)
> Setup
> Ask the user which directory to restrict edits to. Use AskUserQuestion:
> - Question: "Guard mode: which directory should edits be restricted to? Destructive command warnings are always on. Files outside the chosen path will be blocked from editing."
> - Text input (not multiple choice) — the user types a path.
> Once the user provides a directory path:

### 和其他 skill 的潛在關聯
- prioritize-assumptions（pm-skills） - 相似 - 共享領域：safety；共享分類：工具程序型、流程型；共同關鍵詞：what
- pre-mortem（pm-skills） - 相似 - 共享領域：safety；共享分類：流程型；共同關鍵詞：what
- Videos and Courses（awesome-startup） - 相似 - 共享領域：safety；共享分類：工具程序型、流程型
- README（get-shit-done） - 相似 - 共享領域：safety；共享分類：工具程序型、流程型

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
