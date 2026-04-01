---
## oh-my-claudecode / plan

### 來源
- repo：oh-my-claudecode
- 路徑：skills/plan/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/plan/skill.md

### 一句話定位
Strategic planning with optional interview workflow

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- **State lifecycle**: The persistent-mode stop hook uses `ralplan-state.json` to enforce continuation during the consensus loop. The skill **MUST** manage this state:
- 1. **Planner** creates initial plan and a compact **RALPLAN-DR summary** before any Architect review. The summary **MUST** include:
- 2. **User feedback** *(--interactive only)*: If running with `--interactive`, **MUST** use `AskUserQuestion` to present the draft plan **plus the RALPLAN-DR Principles / Decision Drivers / Options summary for early direction alignment** with these options:
- 3. **Architect** reviews for architectural soundness using `Task(subagent_type="oh-my-claudecode:architect", ...)`. Architect review **MUST** include: strongest steelman counterargument (antithesis) against the favored option, at least one meaningful tradeoff tension, and (when possible) a synthesis path. In deliberate mode, Architect should explicitly flag principle violations. **Wait for this step to complete before proceeding to step 4.** Do NOT run steps 3 and 4 in parallel.
- 4. **Critic** evaluates against quality criteria using `Task(subagent_type="oh-my-claudecode:critic", ...)`. Critic **MUST** verify principle-option consistency, fair alternative exploration, risk mitigation clarity, testable acceptance criteria, and concrete verification steps. Critic **MUST** explicitly reject shallow alternatives, driver contradictions, vague risks, or weak verification. In deliberate mode, Critic **MUST** reject missing/weak pre-mortem or missing/weak expanded test plan. Run only after step 3 is complete.
- 6. **Apply improvements**: When reviewers approve with improvement suggestions, merge all accepted improvements into the plan file before proceeding. Final consensus output **MUST** include an **ADR** section with: **Decision**, **Drivers**, **Alternatives considered**, **Why chosen**, **Consequences**, **Follow-ups**. Specifically:
- - **Approve and implement via team**: **MUST** invoke `Skill("oh-my-claudecode:team")` with the approved plan path from `.omc/plans/` as context. Do NOT implement directly. The team skill coordinates parallel agents across the staged pipeline for faster execution on large tasks. This is the recommended default execution path.
- - **Approve and execute via ralph**: **MUST** invoke `Skill("oh-my-claudecode:ralph")` with the approved plan path from `.omc/plans/` as context. Do NOT implement directly. Do NOT edit source code files in the planning agent. The ralph skill handles execution via ultrawork parallel agents.
- - **CRITICAL — Consensus mode agent calls MUST be sequential, never parallel.** Always await the Architect Task result before issuing the Critic Task.
- - In consensus mode with `--interactive`, on user approval **MUST** invoke `Skill("oh-my-claudecode:ralph")` for execution (step 9) -- never implement directly in the planning agent
- - If the user says "just do it" or "skip planning", call `state_write(mode="ralplan", active=false, session_id=<current_session_id>)` then **MUST** invoke `Skill("oh-my-claudecode:ralph")` to transition to execution mode. Do NOT implement directly in the planning agent.
- Before asking any interview question, classify it:

禁止做
- - **CRITICAL — Consensus mode state lifecycle**: Always deactivate ralplan state before stopping or handing off to execution. Use `state_write(active=false)` for handoff paths (approval → ralph/team) and `state_clear` for true terminal exits (rejection, error). Never use `state_clear` before launching an execution mode — its cancel signal disables stop-hook enforcement for 30 seconds.

### 提問方式
- [Question Classification]
- | User Preference | "Priority?", "Timeline?" | Ask user via AskUserQuestion |

### 審查維度
- [Review Quality Criteria]
- | Criterion | Standard |
- |-----------|----------|
- | Clarity | 80%+ claims cite file/line |
- | Testability | 90%+ criteria are concrete |
- | Verification | All file refs exist |
- | Specificity | No vague terms |

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Strategic planning with optional interview workflow」的工作階段使用。
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> Strategic planning with optional interview workflow
> Design Option Presentation
> When presenting design choices during interviews, chunk them:
> 1. **Overview** (2-3 sentences)
> 2. **Option A** with trade-offs
> 3. [Wait for user reaction]
> Question Classification
> Before asking any interview question, classify it:
> Review Quality Criteria
> | Criterion | Standard |

### 和其他 skill 的潛在關聯
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、流程型、元技能型；共同關鍵詞：plan, presentation
- design-html（gstack） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：plan
- free-tool-strategy（marketingskills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：plan
- lead-magnets（marketingskills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：plan

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
