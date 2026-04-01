---
## openai-skills / figma-use

### 來源
- repo：openai-skills
- 路徑：skills/.curated/figma-use/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/figma-use/skill.md

### 一句話定位
" MANDATORY prerequisite — you MUST invoke this skill BEFORE every use figma to…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [5. Incremental Workflow (How to Avoid Bugs)]
- 1. **Inspect first.** Before creating anything, run a read-only `use_figma` to discover what already exists in the file — pages, components, variables, naming conventions. Match what's there.
- 2. **Do one thing per call.** Create variables in one call, create components in the next, compose layouts in another. Don't try to build an entire screen in one script.
- 3. **Return IDs from every call.** Always `return` created node IDs, variable IDs, collection IDs as objects (e.g. `return { createdNodeIds: [...] }`). You'll need these as inputs to subsequent calls.
- 4. **Validate after each step.** Use `get_metadata` to verify structure (counts, names, hierarchy, positions). Use `get_screenshot` after major milestones to catch visual issues.
- 5. **Fix before moving on.** If validation reveals a problem, fix it before proceeding to the next step. Don't build on a broken foundation.
- [7. Pre-Flight Checklist]
- Before submitting ANY `use_figma` call, verify:
- - [ ] Code uses `return` to send data back (NOT `figma.closePlugin()`)
- - [ ] Code is NOT wrapped in an async IIFE (auto-wrapped for you)
- - [ ] `return` value includes structured data with actionable info (IDs, counts)
- - [ ] NO usage of `figma.notify()` anywhere
- - [ ] NO usage of `console.log()` as output (use `return` instead)

### 核心行為規則
必須做
- Before anything, load [plugin-api-standalone.index.md](references/plugin-api-standalone.index.md) to understand what is possible. When you are asked to write plugin API code, use this context to grep [plugin-api-standalone.d.ts](references/plugin-api-standalone.d.ts) for relevant types, methods, and properties. This is the definitive source of truth for the API surface. It is a large typings file, so do not load it all at once, grep for relevant sections as needed.
- 8. Font **MUST** be loaded before any text operation: `await figma.loadFontAsync({family, style})`
- 12. **`layoutSizingHorizontal/Vertical = 'FILL'` MUST be set AFTER `parent.appendChild(child)`** — setting before append throws. Same applies to `'HUG'` on non-auto-layout nodes.
- 15. **MUST `return` ALL created/mutated node IDs.** Whenever a script creates new nodes or mutates existing ones on the canvas, collect every affected node ID and return them in a structured object (e.g. `return { createdNodeIds: [...], mutatedNodeIds: [...] }`). This is essential for subsequent calls to reference, validate, or clean up those nodes.
- - **Returning IDs (CRITICAL)**: Every script that creates or mutates canvas nodes **MUST** return all affected node IDs — e.g. `return { createdNodeIds: [...], mutatedNodeIds: [...] }`. This is a hard requirement, not optional.
- Before submitting ANY `use_figma` call, verify:
- 1. **Inspect first.** Before creating anything, run a read-only `use_figma` to discover what already exists in the file — pages, components, variables, naming conventions. Match what's there.
- ## 8. Discover Conventions Before Creating
- | [gotchas.md](references/gotchas.md) | Before any `use_figma` | Every known pitfall with WRONG/CORRECT code examples |

禁止做
- 17. **`await` every Promise.** Never leave a Promise unawaited — unawaited async calls (e.g. `figma.loadFontAsync(...)` without `await`, or `figma.setCurrentPageAsync(page)` without `await`) will fire-and-forget, causing silent failures or race conditions. The script may return before the async operation completes, leading to missing data or half-applied changes.
- 2. **Do one thing per call.** Create variables in one call, create components in the next, compose layouts in another. Don't try to build an entire screen in one script.
- 5. **Fix before moving on.** If validation reveals a problem, fix it before proceeding to the next step. Don't build on a broken foundation.
- 1. **STOP.** Do not immediately fix the code and retry.
- 4. Write a targeted fix script that modifies only the broken parts — don't recreate everything.
- | [plugin-api-standalone.d.ts](references/plugin-api-standalone.d.ts) | Need exact type signatures | Full typings file — grep for specific symbols, don't load all at once |
- - **Returning IDs (CRITICAL)**: Every script that creates or mutates canvas nodes **MUST** return all affected node IDs — e.g. `return { createdNodeIds: [...], mutatedNodeIds: [...] }`. This is a hard requirement, not optional.
- `use_figma` works in **design mode** (editorType `"figma"`, the default). FigJam (`"figjam"`) has a different set of available node types — most design nodes are blocked there.

### 提問方式
- 3. Identify the discrepancy — is it structural (wrong hierarchy, missing nodes) or visual (wrong colors, broken layout, clipped content)?

### 審查維度
- [7. Pre-Flight Checklist]
- Before submitting ANY `use_figma` call, verify:
- - [ ] Code uses `return` to send data back (NOT `figma.closePlugin()`)
- - [ ] Code is NOT wrapped in an async IIFE (auto-wrapped for you)
- - [ ] `return` value includes structured data with actionable info (IDs, counts)
- - [ ] NO usage of `figma.notify()` anywhere
- - [ ] NO usage of `console.log()` as output (use `return` instead)

### 輸出格式要求
- [3. `return` Is Your Output Channel]
- - **Returning IDs (CRITICAL)**: Every script that creates or mutates canvas nodes **MUST** return all affected node IDs — e.g. `return { createdNodeIds: [...], mutatedNodeIds: [...] }`. This is a hard requirement, not optional.
- - **Progress reporting**: `return { createdNodeIds: [...], count: 5, errors: [] }`
- - **Error info**: Thrown errors are automatically captured and returned — just let them propagate or `throw` explicitly.
- - `console.log()` output is **never** returned to the agent
- - Always return actionable data (IDs, counts, status) so subsequent calls can reference created objects

### 適用場景
- 適合在需要「" MANDATORY prerequisite — you MUST invoke this skill BEFORE every use figma to…」的工作階段使用。
- 常見觸發語句：**MANDATORY prerequisite** — you MUST invoke this skill BEFORE every `use_figma` tool call. NEVER call `use_figma` directly without loading this skill first. Skipping it causes common, hard-to-debug failures. Trigger whenever the user wants to perform a write action or a unique read action that requires JavaScript execution in the Figma file context — e.g. create/edit/delete nodes, set up variables or tokens, build components and variants, modify auto-layout or fills, bind variables to properties, or inspect file structure programmatically.

### 原文精華摘錄
> "**MANDATORY prerequisite** — you MUST invoke this skill BEFORE every `use_figma` tool call
> NEVER call `use_figma` directly without loading this skill first
> Skipping it causes common, hard-to-debug failures
> Trigger whenever the user wants to perform a write action or a unique read action that requires JavaScript execution in the Figma file context — e.g
> create/edit/delete nodes, set up variables or tokens, build components and variants, modify auto-layout or fills, bind variables to properties, or inspect file structure programmatically."
> 1. Critical Rules
> 1. **Use `return` to send data back.** The return value is JSON-serialized automatically (objects, arrays, strings, numbers). Do NOT call `figma.closePlugin()` or wrap code in an async IIFE — this is handled for you.
> 2. **Write plain JavaScript with top-level `await` and `return`.** Code is automatically wrapped in an async context. Do NOT wrap in `(async () => { ... })()`.
> 3. `figma.notify()` **throws "not implemented"** — never use it
> 4. `console.log()` is NOT returned — use `return` for output

### 和其他 skill 的潛在關聯
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：build, checklist, common, examples
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：checklist, common, critical, edit
- docx（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：creating, docs, edit, file
- schema-markup（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：common, output, reference, wants

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
