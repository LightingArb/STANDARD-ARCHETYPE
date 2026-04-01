---
## gstack / freeze

### 來源
- repo：gstack
- 路徑：freeze/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::freeze/skill.md

### 一句話定位
把本次會話的可編輯範圍鎖定在指定目錄的編輯邊界技能。

### 核心人格特質
務實, 操作導向, 證據導向, 懷疑論, 挑戰性, 野心導向, 產品導向, 謹慎, 防呆導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- Ask the user which directory to restrict edits to. Use AskUserQuestion:

禁止做
- a file outside the allowed path will be **blocked** (not just warned).
- - Question: "Which directory should I restrict edits to? Files outside this path will be blocked from editing."
- outside this directory will be blocked. To change the boundary, run `/freeze`

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「把本次會話的可編輯範圍鎖定在指定目錄的編輯邊界技能」的工作階段使用。
- 常見觸發語句：fixing / freeze / restrict edits / only edit this folder / lock down edits
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> Restrict file edits to a specific directory for the session
> Blocks Edit and Write outside the allowed path
> Use when debugging to prevent accidentally "fixing" unrelated code, or when you want to scope changes to one module
> Use when asked to "freeze", "restrict edits", "only edit this folder", or "lock down edits"
> (gstack)
> Setup
> Ask the user which directory to restrict edits to. Use AskUserQuestion:
> - Question: "Which directory should I restrict edits to? Files outside this path will be blocked from editing."
> - Text input (not multiple choice) — the user types a path.
> Once the user provides a directory path:

### 和其他 skill 的潛在關聯
- security-best-practices（openai-skills） - 相似 - 共享領域：safety；共享分類：工具程序型；共同關鍵詞：only, specific
- security-ownership-map（openai-skills） - 相似 - 共享領域：safety；共享分類：工具程序型；共同關鍵詞：notes, only
- security-threat-model（openai-skills） - 相似 - 共享領域：safety；共享分類：工具程序型；共同關鍵詞：only, path
- sora（openai-skills） - 相似 - 共享領域：safety；共享分類：工具程序型；共同關鍵詞：edit, notes

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [ ] 流程型
- [ ] 元技能型
---
