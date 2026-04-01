---
## superpowers / systematic-debugging

### 來源
- repo：superpowers
- 路徑：skills/systematic-debugging/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：superpowers::skills/systematic-debugging/skill.md

### 一句話定位
遇到 bug 時先找根因、再提修復方案的系統化除錯技能。

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 證據導向, 懷疑論

### 核心思考框架
- [Overview]
- **Core principle:** ALWAYS find root cause before attempting fixes. Symptom fixes are failure.
- **Violating the letter of this process is violating the spirit of debugging.**
- [The Iron Law]
- If you haven't completed Phase 1, you cannot propose fixes.
- [The Four Phases]
- You MUST complete each phase before proceeding to the next.
- **BEFORE attempting ANY fix:**
- 1. **Read Error Messages Carefully**
- - Don't skip past errors or warnings
- - They often contain the exact solution
- - Read stack traces completely
- [Red Flags - STOP and Follow Process]
- If you catch yourself thinking:
- - "Quick fix for now, investigate later"
- - "Just try changing X and see if it works"
- - "Add multiple changes, run tests"
- - "Skip the test, I'll manually verify"

### 核心行為規則
必須做
- **Core principle:** ALWAYS find root cause before attempting fixes. Symptom fixes are failure.
- You MUST complete each phase before proceeding to the next.
- - MUST have before fixing
- 3. **Verify Before Continuing**

禁止做
- - You don't fully understand the issue
- **Don't skip when:**
- - Don't skip past errors or warnings
- - If not reproducible → gather more data, don't guess
- - Don't skim - read every line
- - Don't assume "that can't matter"
- - Don't fix multiple things at once
- 4. **When You Don't Know**
- - Say "I don't understand X"
- - Don't pretend to know
- - "I don't fully understand but this might work"
- | "Issue is simple, don't need process" | Simple issues have root causes too. Process is fast for simple bugs. |

### 提問方式
- - Can you trigger it reliably?
- - What are the exact steps?
- - Does it happen every time?
- - What changed that could cause this?
- - Where does bad value originate?
- - What called this with bad value?
- - What works that's similar to what's broken?
- - What's different between working and broken?
- - What other components does this need?
- - What settings, config, environment?
- - What assumptions does it make?
- - Test passes now?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「遇到 bug 時先找根因、再提修復方案的系統化除錯技能」的工作階段使用。

### 原文精華摘錄
> Use when encountering any bug, test failure, or unexpected behavior, before proposing fixes
> Overview
> **Core principle:** ALWAYS find root cause before attempting fixes. Symptom fixes are failure.
> **Violating the letter of this process is violating the spirit of debugging.**
> The Iron Law
> If you haven't completed Phase 1, you cannot propose fixes.
> When to Use
> Use for ANY technical issue:
> - Test failures
> - Bugs in production

### 和其他 skill 的潛在關聯
- investigate（gstack） - 相似 - 共享領域：debugging；共享分類：思考框架型、流程型；共同關鍵詞：behavior, bug, cause, fixes
- qa（gstack） - 相似 - 共享領域：debugging；共享分類：流程型；共同關鍵詞：fixes, phases, quick, test
- webapp-testing（anthropics-skills） - 相似 - 共享領域：debugging；共享分類：思考框架型、流程型；共同關鍵詞：behavior, common, reference
- qa-only（gstack） - 相似 - 共享領域：debugging；共享分類：流程型；共同關鍵詞：bug, fixes, test

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
