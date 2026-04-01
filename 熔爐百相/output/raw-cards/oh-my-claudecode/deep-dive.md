---
## oh-my-claudecode / deep-dive

### 來源
- repo：oh-my-claudecode
- 路徑：skills/deep-dive/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/deep-dive/skill.md

### 一句話定位
"2-stage pipeline: trace (causal investigation) - deep-interview (requirements…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Phase 1: Initialize]
- 1. **Parse the user's idea** from `{{ARGUMENTS}}`
- 2. **Generate slug**: kebab-case from first 5 words of ARGUMENTS, lowercased, special characters stripped. Example: "Why does the auth token expire early?" becomes `why-does-the-auth-token`
- 3. **Detect brownfield vs greenfield**:
- - Run `explore` agent (haiku): check if cwd has existing source code, package files, or git history
- - If source files exist AND the user's idea references modifying/extending something: **brownfield**
- - Otherwise: **greenfield**
- [Phase 2: Lane Confirmation]
- Present the 3 hypotheses to the user via `AskUserQuestion` for confirmation (1 round only):
- **Options:**
- - Confirm and start trace
- - Adjust hypotheses (user provides alternatives)
- [Phase 3: Trace Execution]
- Use **Claude built-in team mode** to run 3 parallel tracer lanes:
- [Phase 4: Interview with Trace Injection]
- Phase 4 follows the `oh-my-claudecode:deep-interview` SKILL.md Phases 2-4 (Interview Loop, Challenge Agents, Crystallize Spec) as the base behavioral contract. The executor MUST read the deep-interview SKILL.md to understand the full interview protocol. Deep-dive does NOT duplicate the interview protocol — it specifies exactly **3 initialization overrides**:
- **Override 1 — initial_idea enrichment**: Replace deep-interview's raw `{{ARGUMENTS}}` initialization with:
- **Override 2 — codebase_context replacement**: Skip deep-interview's Phase 1 brownfield explore step. Instead, set `codebase_context` in state to the full trace synthesis (wrapped in `<trace-context>` delimiters). The trace already mapped the relevant system areas with evidence — re-exploring would be redundant.

### 核心行為規則
必須做
- Phase 4 follows the `oh-my-claudecode:deep-interview` SKILL.md Phases 2-4 (Interview Loop, Challenge Agents, Crystallize Spec) as the base behavioral contract. The executor MUST read the deep-interview SKILL.md to understand the full interview protocol. Deep-dive does NOT duplicate the interview protocol — it specifies exactly **3 initialization overrides**:
- **IMPORTANT:** On execution selection, **MUST** invoke the chosen skill via `Skill()` with explicit `spec_path`. Do NOT implement directly. The deep-dive skill is a requirements pipeline, not an execution agent.

禁止做
- - Do not proceed to execution — always hand off via Execution Bridge (Phase 5)

### 提問方式
- > Are these hypotheses appropriate, or would you like to adjust them?

### 審查維度
非審查型

### 輸出格式要求
- [Evidence Summary by Hypothesis]
- - **Hypothesis 1**: ...
- - **Hypothesis 2**: ...
- - **Hypothesis 3**: ...

### 適用場景
- 適合在需要「"2-stage pipeline: trace (causal investigation) - deep-interview (requirements…」的工作階段使用。
- 常見觸發語句：2-stage pipeline: trace (causal investigation) -> deep-interview (requirements crystallization) with 3-point injection
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> "2-stage pipeline: trace (causal investigation) -> deep-interview (requirements crystallization) with 3-point injection"
> Phase 1: Initialize
> 1. **Parse the user's idea** from `{{ARGUMENTS}}`
> 2. **Generate slug**: kebab-case from first 5 words of ARGUMENTS, lowercased, special characters stripped. Example: "Why does the auth token expire early?" becomes `why-does-the-auth-token`
> 3. **Detect brownfield vs greenfield**:
> - Run `explore` agent (haiku): check if cwd has existing source code, package files, or git history
> Phase 2: Lane Confirmation
> Present the 3 hypotheses to the user via `AskUserQuestion` for confirmation (1 round only):
> **Options:**
> - Confirm and start trace

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：critical, existing, integration, pipeline
- lean-canvas（pm-skills） - 相似 - 共享領域：debugging, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：hypothesis, notes, requirements
- ab-test-setup（marketingskills） - 相似 - 共享領域：debugging, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：hypothesis
- spreadsheet（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：existing, missing, requirements

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
