---
## superpowers / test-driven-development

### 來源
- repo：superpowers
- 路徑：skills/test-driven-development/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/test-driven-development/skill.md

### 一句話定位
在寫實作前先寫失敗測試，再按 red-green-refactor 前進的 TDD 技能。

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 證據導向, 懷疑論

### 核心思考框架
- [Overview]
- **Core principle:** If you didn't watch the test fail, you don't know if it tests the right thing.
- **Violating the letter of the rules is violating the spirit of the rules.**
- [The Iron Law]
- **No exceptions:**
- - Don't keep it as "reference"
- - Don't "adapt" it while writing tests
- - Don't look at it
- - Delete means delete
- [Verification Checklist]
- Before marking work complete:
- - [ ] Every new function/method has a test
- - [ ] Watched each test fail before implementing
- - [ ] Each test failed for expected reason (feature missing, not typo)
- - [ ] Wrote minimal code to pass each test
- - [ ] All tests pass
- [Testing Anti-Patterns]
- When adding mocks or test utilities, read @testing-anti-patterns.md to avoid common pitfalls:

### 核心行為規則
必須做
- Before marking work complete:

禁止做
- **Core principle:** If you didn't watch the test fail, you don't know if it tests the right thing.
- - Don't keep it as "reference"
- - Don't "adapt" it while writing tests
- - Don't look at it
- **MANDATORY. Never skip.**
- Don't add features, refactor other code, or "improve" beyond the test.
- Keep tests green. Don't add behavior.
- | Don't know how to test | Write wished-for API. Write assertion first. Ask your human partner. |
- Never fix bugs without a test.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「在寫實作前先寫失敗測試，再按 red-green-refactor 前進的 TDD 技能」的工作階段使用。

### 原文精華摘錄
> Use when implementing any feature or bugfix, before writing implementation code
> Overview
> **Core principle:** If you didn't watch the test fail, you don't know if it tests the right thing.
> **Violating the letter of the rules is violating the spirit of the rules.**
> When to Use
> **Always:**
> - New features
> - Bug fixes
> - Refactoring
> The Iron Law

### 和其他 skill 的潛在關聯
- investigate（gstack） - 相似 - 共享領域：debugging；共享分類：思考框架型、流程型；共同關鍵詞：bug, fix, iron, law
- qa（gstack） - 相似 - 共享領域：debugging, testing；共享分類：流程型；共同關鍵詞：feature, final, fix, testing
- webapp-testing（anthropics-skills） - 相似 - 共享領域：debugging, testing；共享分類：思考框架型、流程型；共同關鍵詞：common, example, testing
- ab-test-setup（marketingskills） - 相似 - 共享領域：debugging, testing；共享分類：思考框架型、流程型；共同關鍵詞：common

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
