---
## pm-skills / ab-test-analysis

### 來源
- repo：pm-skills
- 路徑：pm-data-analytics/skills/ab-test-analysis/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：pm-skills::pm-data-analytics/skills/ab-test-analysis/skill.md

### 一句話定位
"Analyze A/B test results with statistical significance, sample size validation…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 證據導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- | Significant negative lift | **Don't ship** — revert to control, analyze why |

### 提問方式
- - What was the hypothesis?
- - What was changed (the variant)?
- - What is the primary metric? Any guardrail metrics?
- - How long did the test run?
- - What is the traffic split?
- - **Sample size**: Is the sample large enough for the expected effect size?
- - **Duration**: Did the test run for at least 1-2 full business cycles?
- - **Randomization**: Any evidence of sample ratio mismatch (SRM)?
- - **Novelty/primacy effects**: Was there enough time to wash out initial behavior changes?
- - **Statistical significance**: Is p < 0.05?
- - **Practical significance**: Is the lift meaningful for the business?
- - Did any guardrail metrics (revenue, engagement, page load time) degrade?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Analyze A/B test results with statistical significance, sample size validation…」的工作階段使用。
- 常見觸發語句：Analyze A/B test results with statistical significance, sample size validation, confidence intervals, and ship/extend/stop recommendations. Use when evaluating experiment results, checking if a test reached significance, interpreting split test data, or deciding whether to ship a variant.
- 常出現在收尾、發布、分支整理或部署交接階段。

### 原文精華摘錄
> "Analyze A/B test results with statistical significance, sample size validation, confidence intervals, and ship/extend/stop recommendations
> Use when evaluating experiment results, checking if a test reached significance, interpreting split test data, or deciding whether to ship a variant."
> A/B Test Analysis
> 1. **Understand the experiment**:
> - What was the hypothesis?
> - What was changed (the variant)?
> - What is the primary metric? Any guardrail metrics?

### 和其他 skill 的潛在關聯
- ab-test-setup（marketingskills） - 相似 - 共享領域：meta, shipping, testing；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：experiment, results, sample, significance
- skill-creator（anthropics-skills） - 相似 - 共享領域：meta, shipping, testing；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：analysis, evaluating, recommendations, test
- analytics-tracking（marketingskills） - 相似 - 共享領域：meta, shipping, testing；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：results, test, validation
- schema-markup（marketingskills） - 相似 - 共享領域：meta, shipping, testing；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：data, results, validation

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
