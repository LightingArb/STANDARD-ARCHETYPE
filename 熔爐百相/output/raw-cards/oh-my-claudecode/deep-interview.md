---
## oh-my-claudecode / deep-interview

### 來源
- repo：oh-my-claudecode
- 路徑：skills/deep-interview/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/deep-interview/skill.md

### 一句話定位
Socratic deep interview with mathematical ambiguity gating before autonomous ex…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Phase 1: Initialize]
- 1. **Parse the user's idea** from `{{ARGUMENTS}}`
- 2. **Detect brownfield vs greenfield**:
- - Run `explore` agent (haiku): check if cwd has existing source code, package files, or git history
- - If source files exist AND the user's idea references modifying/extending something: **brownfield**
- - Otherwise: **greenfield**
- 3. **For brownfield**: Run `explore` agent to map relevant codebase areas, store as `codebase_context`
- [Phase 2: Interview Loop]
- Repeat until `ambiguity ≤ threshold` OR user exits early:
- Build the question generation prompt with:
- - The user's original idea
- - All prior Q&A rounds (conversation history)
- - Current clarity scores per dimension (which is weakest?)
- - Challenge agent mode (if activated -- see Phase 3)
- [Phase 4: Crystallize Spec]
- When ambiguity ≤ threshold (or hard cap / early exit):
- 1. **Generate the specification** using opus model with the full interview transcript
- 2. **Write to file**: `.omc/specs/deep-interview-{slug}.md`

### 核心行為規則
必須做
- **IMPORTANT:** On execution selection, **MUST** invoke the chosen skill via `Skill()`. Do NOT implement directly. The deep-interview agent is a requirements agent, not an execution agent.
- **Show your work:** Before reporting stability numbers, briefly list which entities were matched (by name or fuzzy) and which are new/removed. This lets the user sanity-check the matching.

禁止做
- - User says "deep interview", "interview me", "ask me everything", "don't assume", "make sure you understand"
- - Do not proceed to execution until ambiguity ≤ threshold (default 0.2)
- Why good: Explored first, cited the repo evidence that triggered the question, then asked an informed confirmation question. Never asks the user what the code already reveals.
- Why bad: Should have spawned explore agent to find this. Never ask the user what the code already tells you.

### 提問方式
- 1. **Deep Interview** gates on *clarity* — does the user know what they want?
- 2. **Ralplan** gates on *feasibility* — is the approach architecturally sound?
- 3. **Autopilot** gates on *correctness* — does the code work and pass review?

### 審查維度
- [Ambiguity Score Interpretation]
- | Score Range | Meaning | Action |
- |-------------|---------|--------|
- | 0.0 - 0.1 | Crystal clear | Proceed immediately |
- | 0.1 - 0.2 | Clear enough | Proceed (default threshold) |
- | 0.2 - 0.4 | Some gaps | Continue interviewing |
- | 0.4 - 0.6 | Significant gaps | Focus on weakest dimensions |

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Socratic deep interview with mathematical ambiguity gating before autonomous ex…」的工作階段使用。

### 原文精華摘錄
> Socratic deep interview with mathematical ambiguity gating before autonomous execution
> Phase 1: Initialize
> 1. **Parse the user's idea** from `{{ARGUMENTS}}`
> 2. **Detect brownfield vs greenfield**:
> - Run `explore` agent (haiku): check if cwd has existing source code, package files, or git history
> - If source files exist AND the user's idea references modifying/extending something: **brownfield**
> Phase 2: Interview Loop
> Repeat until `ambiguity ≤ threshold` OR user exits early:
> Build the question generation prompt with:
> - The user's original idea
> - All prior Q&A rounds (conversation history)
> Phase 3: Challenge Agents
> At specific round thresholds, shift the questioning perspective:
> Inject into the question generation prompt:
> Phase 4: Crystallize Spec
> When ambiguity ≤ threshold (or hard cap / early exit):

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：integration, pipeline
- lead-magnets（marketingskills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：gating, stage
- create-prd（pm-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、流程型、元技能型；共同關鍵詞：context, spec
- design-html（gstack） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：loop

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
