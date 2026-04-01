---
## superpowers / verification-before-completion

### 來源
- repo：superpowers
- 路徑：skills/verification-before-completion/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/verification-before-completion/skill.md

### 一句話定位
在宣稱完成前先跑驗證並以證據支撐結論的收尾技能。

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 證據導向, 懷疑論

### 核心思考框架
- [Overview]
- **Core principle:** Evidence before claims, always.
- **Violating the letter of this rule is violating the spirit of this rule.**
- [The Iron Law]
- If you haven't run the verification command in this message, you cannot claim it passes.
- [Key Patterns]
- **Tests:**
- **Regression tests (TDD Red-Green):**
- **Build:**
- **Requirements:**
- **Agent delegation:**

### 核心行為規則
必須做
- ✅ Write → Run (pass) → Revert fix → Run (MUST FAIL) → Restore → Run (pass)
- **ALWAYS before:**
- # Verification Before Completion

禁止做
- - your human partner said "I don't believe you" - trust broken
- If you haven't run the verification command in this message, you cannot claim it passes.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「在宣稱完成前先跑驗證並以證據支撐結論的收尾技能」的工作階段使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Use when about to claim work is complete, fixed, or passing, before committing or creating PRs - requires running verification commands and confirming output before making any success claims
> evidence before assertions always
> Overview
> **Core principle:** Evidence before claims, always.
> **Violating the letter of this rule is violating the spirit of this rule.**
> The Iron Law
> If you haven't run the verification command in this message, you cannot claim it passes.
> The Gate Function
> Common Failures
> | Claim | Requires | Not Sufficient |

### 和其他 skill 的潛在關聯
- ship（gstack） - 相似 - 共享領域：review, shipping, testing；共享分類：審查型、流程型；共同關鍵詞：about, gate, key, verification
- analytics-tracking（marketingskills） - 相似 - 共享領域：review, shipping, testing；共享分類：思考框架型、審查型、流程型；共同關鍵詞：output, overview
- schema-markup（marketingskills） - 相似 - 共享領域：review, shipping, testing；共享分類：思考框架型、審查型、流程型；共同關鍵詞：common, output
- figma-use（openai-skills） - 相似 - 共享領域：review, shipping；共享分類：思考框架型、審查型、流程型；共同關鍵詞：common, creating, failures, output

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
