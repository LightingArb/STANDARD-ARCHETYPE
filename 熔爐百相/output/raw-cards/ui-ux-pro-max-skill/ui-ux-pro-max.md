---
## ui-ux-pro-max-skill / ui-ux-pro-max

### 來源
- repo：ui-ux-pro-max-skill
- 路徑：.claude/skills/ui-ux-pro-max/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：ui-ux-pro-max-skill::.claude/skills/ui-ux-pro-max/skill.md

### 一句話定位
"UI/UX design intelligence for web and mobile.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Example Workflow]
- **User request:** "Make an AI search homepage."
- - Product type: Tool (AI search engine)
- - Target audience: C-end users looking for fast, intelligent search
- - Style keywords: modern, minimal, content-first, dark mode
- - Stack: React Native
- **Output:** Complete design system with pattern, style, colors, typography, effects, and anti-patterns.
- [Pre-Delivery Checklist]
- Before delivering UI code, verify these items:
- - [ ] No emojis used as icons (use SVG instead)
- - [ ] All icons come from a consistent icon family and style
- - [ ] Official brand assets are used with correct proportions and clear space
- - [ ] Pressed-state visuals do not shift layout bounds or cause jitter
- - [ ] Semantic theme tokens are used consistently (no ad-hoc per-screen hardcoded colors)

### 核心行為規則
必須做
- ### Step 2: Generate Design System (REQUIRED)
- Before delivering UI code, verify these items:

禁止做
- - `color-not-only` - Don't convey info by color alone (add icon/text)
- - `hover-vs-tap` - Use click/tap for primary interactions; don't rely on hover alone
- - `standard-gestures` - Use platform standard gestures consistently; don't redefine (e.g. swipe-back, pinch-zoom) (Apple HIG)
- - `system-gestures` - Don't block system gestures (Control Center, back swipe, etc.) (Apple HIG)
- - `gesture-alternative` - Don't rely on gesture-only interactions; always provide visible controls for critical actions
- - `no-blocking-animation` - Never block user input during an animation; UI must stay interactive (Apple HIG)
- - `progressive-disclosure` - Reveal complex options progressively; don't overwhelm users upfront (Apple HIG)
- - `back-stack-integrity` - Never silently reset the navigation stack or unexpectedly jump to home (HIG, MD)
- - `navigation-consistency` - Navigation placement must stay the same across all pages; don't change by page type
- - `avoid-mixed-patterns` - Don't mix Tab + Sidebar + Bottom Nav at the same hierarchy level
- - `persistent-nav` - Core navigation must remain reachable from deep pages; don't hide it entirely in sub-flows (HIG, MD)
- - `loading-chart` - Use skeleton or shimmer placeholder while chart data loads; don't show an empty axis frame

### 提問方式
無明確提問模板

### 審查維度
- [Pre-Delivery Checklist]
- Before delivering UI code, verify these items:
- - [ ] No emojis used as icons (use SVG instead)
- - [ ] All icons come from a consistent icon family and style
- - [ ] Official brand assets are used with correct proportions and clear space
- - [ ] Pressed-state visuals do not shift layout bounds or cause jitter
- - [ ] Semantic theme tokens are used consistently (no ad-hoc per-screen hardcoded colors)

### 輸出格式要求
- [Output Formats]
- The `--design-system` flag supports two output formats:

### 適用場景
- 適合在需要「"UI/UX design intelligence for web and mobile.」的工作階段使用。
- 常見觸發語句：UI/UX design intelligence for web and mobile. Includes 50+ styles, 161 color palettes, 57 font pairings, 161 product types, 99 UX guidelines, and 25 chart types across 10 stacks (React, Next.js, Vue, Svelte, SwiftUI, React Native, Flutter, Tailwind, shadcn/ui, and HTML/CSS). Actions: plan, build, create, design, implement, review, fix, improve, optimize, enhance, refactor, and check UI/UX code. Projects: website, landing page, dashboard, admin panel, e-commerce, SaaS, portfolio, blog, and mobile app. Elements: button, modal, navbar, sidebar, card, table, form, and chart. Styles: glassmorphism, claymorphism, minimalism, brutalism, neumorphism, bento grid, dark mode, responsive, skeuomorphism, and flat design. Topics: color systems, accessibility, animation, layout, typography, font pairing, spacing, interaction states, shadow, and gradient. Integrations: shadcn/ui MCP for component search and examples.
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> "UI/UX design intelligence for web and mobile
> Includes 50+ styles, 161 color palettes, 57 font pairings, 161 product types, 99 UX guidelines, and 25 chart types across 10 stacks (React, Next.js, Vue, Svelte, SwiftUI, React Native, Flutter, Tailwind, shadcn/ui, an...
> Actions: plan, build, create, design, implement, review, fix, improve, optimize, enhance, refactor, and check UI/UX code
> Projects: website, landing page, dashboard, admin panel, e-commerce, SaaS, portfolio, blog, and mobile app
> Elements: button, modal, navbar, sidebar, card, table, form, and chart
> Styles: glassmorphism, claymorphism, minimalism, brutalism, neumorphism, bento grid, dark mode, responsive, skeuomorphism, and flat design
> Topics: color systems, accessibility, animation, layout, typography, font pairing, spacing, interaction states, shadow, and gradient
> Integrations: shadcn/ui MCP for component search and examples."
> When to Apply
> This Skill must be invoked in the following situations:
> - Designing new pages (Landing Page, Dashboard, Admin, SaaS, Mobile App)
> - Creating or refactoring UI components (buttons, modals, forms, tables, charts, etc.)
> - Choosing color schemes, typography systems, spacing standards, or layout systems
> Rule Categories by Priority
> *For human/AI reference: follow priority 1→10 to decide which rule category to focus on first; use `--domain <Domain>` to query details when needed. Scripts do not read this table.*
> | Priority | Category | Impact | Domain | Key Checks (Must Have) | Anti-Patterns (Avoid) |

### 和其他 skill 的潛在關聯
- schema-markup（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：common, fix, optimize, output
- lead-magnets（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：checklist, landing, optimize, output
- design-html（gstack） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：css, html, implement, next
- react-best-practices（vercel-agent-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：apply, categories, guidelines, next

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [x] 人格型
- [x] 流程型
- [x] 元技能型
---
