---
## openai-skills / figma-generate-library

### 來源
- repo：openai-skills
- 路徑：skills/.curated/figma-generate-library/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/figma-generate-library/skill.md

### 一句話定位
"Build or update a professional-grade design system in Figma from a codebase.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [2. Mandatory Workflow]
- Every design system build follows this phase order. Skipping or reordering phases causes structural failures that are expensive to undo.
- [4. State Management (Required for Long Workflows)]
- Tag every created **scene node** immediately after creation:
- **State persistence**: Do NOT rely solely on conversation context for the state ledger. Write it to disk:
- Maintain a state ledger tracking:
- **Idempotency check** before every create: query by name + state ledger ID. If exists, skip or update — never duplicate.
- **Resume protocol**: at session start or after context truncation, run a read-only `use_figma` to scan all pages, components, variables, and styles by name to reconstruct the `{key → id}` map. Then re-read the state file from disk if available.
- **Continuation prompt** (give this to the user when resuming in a new chat):
- [5. search_design_system — Reuse Decision Matrix]
- **Reuse if** all of these are true:
- - Component property API matches your needs (same variant axes, compatible types)
- - Token binding model is compatible (uses same or aliasable variables)
- - Naming conventions match the target file
- - Component is editable (not locked in a remote library you don't own)
- **Rebuild if** any of these:
- [9. Per-Phase Anti-Patterns]
- **Phase 0 anti-patterns:**

### 核心行為規則
必須做
- **Prerequisites**: The `figma-use` skill MUST also be loaded for every `use_figma` call. It provides Plugin API syntax rules (return pattern, page reset, ID return, font loading, color range). This skill provides design system domain knowledge and workflow orchestration.
- - Font MUST be loaded before any text write: `await figma.loadFontAsync({family, style})`
- 6. **Code syntax on every variable** — WEB syntax MUST use the `var()` wrapper: `var(--color-bg-primary)`, not `--color-bg-primary`. Use the actual CSS variable name from the codebase. ANDROID/iOS do NOT use a wrapper.

禁止做
- **This is NEVER a one-shot task.** Building a design system requires 20–100+ `use_figma` calls across multiple phases, with mandatory user checkpoints between them. Any attempt to create everything in one call WILL produce broken, incomplete, or unrecoverable results. Break every operation to the smallest useful unit, validate, get feedback, proceed.
- 5. **Scopes on every variable** — NEVER leave as `ALL_SCOPES`. Background: `FRAME_FILL, SHAPE_FILL`. Text: `TEXT_FILL`. Border: `STROKE_COLOR`. Spacing: `GAP`. Radii: `CORNER_RADIUS`. Primitives: `[]` (hidden).
- 7. **Alias semantics to primitives** — `{ type: 'VARIABLE_ALIAS', id: primitiveVar.id }`. Never duplicate raw values in semantic layer.
- 13. **NEVER parallelize `use_figma` calls** — Figma state mutations must be strictly sequential. Even if your tool supports parallel calls, never run two use_figma calls simultaneously.
- 14. **Never hallucinate Node IDs** — always read IDs from the state ledger returned by previous calls. Never reconstruct or guess an ID from memory.
- 15. **Use the helper scripts** — embed scripts from `scripts/` into your use_figma calls. Don't write 200-line inline scripts from scratch.
- - Component is editable (not locked in a remote library you don't own)
- **If user rejects**: fix before moving on. Never build on rejected work.
- Use your file reading tool to read these docs when needed. Do not assume their contents from the filename.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Build or update a professional-grade design system in Figma from a codebase.」的工作階段使用。
- 常見觸發語句：Build or update a professional-grade design system in Figma from a codebase. Use when the user wants to create variables/tokens, build component libraries, set up theming (light/dark modes), document foundations, or reconcile gaps between code and Figma. This skill teaches WHAT to build and in WHAT ORDER — it complements the `figma-use` skill which teaches HOW to call the Plugin API. Both skills should be loaded together.

### 原文精華摘錄
> "Build or update a professional-grade design system in Figma from a codebase
> Use when the user wants to create variables/tokens, build component libraries, set up theming (light/dark modes), document foundations, or reconcile gaps between code and Figma
> This skill teaches WHAT to build and in WHAT ORDER — it complements the `figma-use` skill which teaches HOW to call the Plugin API
> Both skills should be loaded together."
> 1. The One Rule That Matters Most
> **This is NEVER a one-shot task.** Building a design system requires 20–100+ `use_figma` calls across multiple phases, with mandatory user checkpoints between them. Any attempt to create everything in one call WILL produce broken, incomplete, or unrecoverab...
> 2. Mandatory Workflow
> Every design system build follows this phase order. Skipping or reordering phases causes structural failures that are expensive to undo.
> 3. Critical Rules
> **Plugin API basics** (from use_figma skill — enforced here too):

### 和其他 skill 的潛在關聯
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：build, component, dark, reference
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：api, architecture, build, reference
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：api, between, critical, document
- design-system（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、工具程序型、元技能型；共同關鍵詞：architecture, component, scripts, system

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
