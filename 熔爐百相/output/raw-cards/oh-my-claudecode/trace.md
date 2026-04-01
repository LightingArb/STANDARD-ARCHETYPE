---
## oh-my-claudecode / trace

### 來源
- repo：oh-my-claudecode
- 路徑：skills/trace/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/trace/skill.md

### 一句話定位
Evidence-driven tracing lane that orchestrates competing tracer hypotheses in C…

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 角色鮮明, 觀點強烈, 規範導向, 元認知

### 核心思考框架
- [Evidence strength hierarchy]
- From strongest to weakest:
- 1. **Controlled reproductions / direct experiments / uniquely discriminating artifacts**
- 2. **Primary source artifacts with tight provenance** (trace events, logs, metrics, benchmark outputs, configs, git history, file:line behavior)
- 3. **Multiple independent sources converging on the same explanation**
- 4. **Single-source code-path or behavioral inference**
- 5. **Weak circumstantial clues** (timing, naming, stack order, resemblance to prior bugs)

### 核心行為規則
必須做
- Before closing the trace:

禁止做
- Do not claim convergence just because multiple workers use similar language. Convergence requires either:

### 提問方式
- - **Premortem lens** -- assume the current best explanation is incomplete or wrong; what failure mode would embarrass the trace later?

### 審查維度
非審查型

### 輸出格式要求
- [Output quality bar]
- Good `/trace` output is:
- - evidence-backed
- - concise but rigorous
- - skeptical of premature certainty
- - explicit about missing evidence
- - practical about the next action

### 適用場景
- 適合在需要「Evidence-driven tracing lane that orchestrates competing tracer hypotheses in C…」的工作階段使用。

### 原文精華摘錄
> Evidence-driven tracing lane that orchestrates competing tracer hypotheses in Claude built-in team mode
> Good entry cases
> Use `/oh-my-claudecode:trace` when the problem is:
> - ambiguous
> - causal
> - evidence-heavy
> Core tracing contract
> Always preserve these distinctions:
> 1. **Observation** -- what was actually observed
> 2. **Hypotheses** -- competing explanations

### 和其他 skill 的潛在關聯
- frontend-skill（openai-skills） - 相似 - 共享領域：debugging, design, meta；共享分類：工具程序型、人格型、流程型、元技能型；共同關鍵詞：hierarchy, rules, strong
- figma-use（openai-skills） - 相似 - 共享領域：debugging, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：mandatory, output, rules
- ab-test-setup（marketingskills） - 相似 - 共享領域：debugging, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：core, hypothesis
- lean-canvas（pm-skills） - 相似 - 共享領域：debugging, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：hypothesis, output

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [x] 人格型
- [x] 流程型
- [x] 元技能型
---
