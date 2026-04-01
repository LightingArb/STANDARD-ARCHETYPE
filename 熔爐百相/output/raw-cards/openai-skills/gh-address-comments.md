---
## openai-skills / gh-address-comments

### 來源
- repo：openai-skills
- 路徑：skills/.curated/gh-address-comments/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/gh-address-comments/skill.md

### 一句話定位
Help address review/issue comments on the open GitHub PR for the current branch…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- ## 2) Ask the user for clarification
- - Ask the user which numbered comments should be addressed

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
- Help address review/issue comments on the open GitHub PR for the current branch using gh CLI

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Help address review/issue comments on the open GitHub PR for the current branch…」的工作階段使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Help address review/issue comments on the open GitHub PR for the current branch using gh CLI
> verify gh auth first and prompt the user to authenticate if not logged in
> 1) Inspect comments needing attention
> - Run scripts/fetch_comments.py which will print out all the comments and review threads on the PR
> 2) Ask the user for clarification
> - Number all the review threads and comments and provide a short summary of what would be required to apply a fix for it
> - Ask the user which numbered comments should be addressed
> 3) If user chooses comments
> - Apply fixes for the selected comments
> Notes:

### 和其他 skill 的潛在關聯
- ship（gstack） - 相似 - 共享領域：review, shipping；共享分類：審查型、工具程序型、流程型；共同關鍵詞：address, branch, comments, first
- canary（gstack） - 相似 - 共享領域：review, shipping；共享分類：審查型、工具程序型、流程型；共同關鍵詞：branch, first, verify
- land-and-deploy（gstack） - 相似 - 共享領域：review, shipping；共享分類：審查型、工具程序型、流程型；共同關鍵詞：branch, first, verify
- review（gstack） - 相似 - 共享領域：review, shipping；共享分類：審查型、流程型；共同關鍵詞：branch, comments, first

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
