---
## vercel-agent-skills / react-view-transitions

### 來源
- repo：vercel-agent-skills
- 路徑：skills/react-view-transitions/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：vercel-agent-skills::skills/react-view-transitions/skill.md

### 一句話定位
Guide for implementing smooth, native-feeling animations using React's View Tra…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Implementation Workflow]
- When adding view transitions to an existing app, **follow `references/implementation.md` step by step.** Start with the audit — do not skip it. Copy the CSS recipes from `references/css-recipes.md` into the global stylesheet — do not write your own animation CSS.
- [Common Patterns]
- **Caution:** If wrapping `<Suspense>`, changing `key` remounts the boundary and refetches.
- Simple cross-fade:
- Directional reveal:

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- Every `<ViewTransition>` should communicate a spatial relationship or continuity. If you can't articulate what it communicates, don't add it.
- React auto-assigns a unique `view-transition-name` and calls `document.startViewTransition` behind the scenes. Never call `startViewTransition` yourself.
- - Never use a fade-out exit on pages with shared morphs — use a directional slide instead.
- Every VT matching the trigger fires simultaneously in a single `document.startViewTransition`. VTs in **different** transitions (navigation vs later Suspense resolve) don't compete.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Guide for implementing smooth, native-feeling animations using React's View Tra…」的工作階段使用。

### 原文精華摘錄
> Guide for implementing smooth, native-feeling animations using React's View Transition API (`<ViewTransition>` component, `addTransitionType`, and CSS view transition pseudo-elements)
> Use this skill whenever the user wants to add page transitions, animate route changes, create shared element animations, animate enter/exit of components, animate list reorder, implement directional (forward/back) nav...
> Also use when the user mentions view transitions, `startViewTransition`, `ViewTransition`, transition types, or asks about animating between UI states in React without third-party animation libraries
> When to Animate
> Implement **all** applicable patterns from this list, in this order:
> Availability
> - Requires `react@canary` or `react@experimental` — **not** in stable React (including 19.x). Verify with `npm ls react`.
> - Browser support: Chromium 111+, Firefox 144+, Safari 18.2+. Graceful degradation on unsupported browsers.
> Implementation Workflow
> When adding view transitions to an existing app, **follow `references/implementation.md` step by step.** Start with the audit — do not skip it. Copy the CSS recipes from `references/css-recipes.md` into the global stylesheet — do not write your own animatio...

### 和其他 skill 的潛在關聯
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：accessibility, animation, common, component
- ui-styling（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：accessibility, common, component, core
- schema-markup（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：add, also, common, core
- popup-cro（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：accessibility, also, common, core

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
