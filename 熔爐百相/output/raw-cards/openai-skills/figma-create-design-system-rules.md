---
## openai-skills / figma-create-design-system-rules

### 來源
- repo：openai-skills
- 路徑：skills/.curated/figma-create-design-system-rules/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/figma-create-design-system-rules/skill.md

### 一句話定位
Generates custom design system rules for the user's codebase.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Overview]
- This skill helps you generate custom design system rules tailored to your project's specific needs. These rules guide AI coding agents to produce consistent, high-quality code when implementing Figma designs, ensuring that your team's conventions, component patterns, and architectural decisions are followed automatically.
- ### Supported Rule Files
- | Agent | Rule File |
- |-------|-----------|
- | Claude Code | `CLAUDE.md` |
- | Codex CLI | `AGENTS.md` |
- [Required Workflow]
- **Follow these steps in order. Do not skip steps.**
- **Parameters:**
- - `clientLanguages`: Comma-separated list of languages used in the project (e.g., "typescript,javascript", "python", "javascript")
- - `clientFrameworks`: Framework being used (e.g., "react", "vue", "svelte", "angular", "unknown")
- Before finalizing rules, analyze the project to understand existing patterns:
- **Component Organization:**
- [Figma Implementation Flow]
- 1. Run get_design_context for the node
- 2. Run get_screenshot for visual reference
- 3. Map Figma colors to Tailwind colors defined in `tailwind.config.js`

### 核心行為規則
必須做
- Before finalizing rules, analyze the project to understand existing patterns:
- **Before rules:**

禁止做
- **Follow these steps in order. Do not skip steps.**
- - IMPORTANT: Never hardcode colors - always use tokens from `[TOKEN_FILE]`
- - IMPORTANT: DO NOT import/add new icon packages - all assets should be in the Figma payload
- - IMPORTANT: DO NOT use or create placeholders if a localhost source is provided
- - Never hardcode hex colors - use `var(--color-*)` tokens
- - DO NOT install new icon libraries
- - IMPORTANT: Never hardcode values - import from tokens package
- Don't try to capture every rule upfront. Start with the most important conventions and add rules as you encounter inconsistencies.
- Bad: "Don't hardcode colors"
- - IMPORTANT: Never expose API keys in client-side code
- **Cause:** Codebase changes but rules don't.
- - Hardcoded values that don't match design tokens

### 提問方式
- - Is there a dedicated design system directory?
- - Are there existing color, typography, or spacing tokens?
- - How are component props typically structured?
- - Are there common composition patterns?
- - How is state management handled?
- - What routing system is used?
- - Are there specific import patterns or path aliases?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Generates custom design system rules for the user's codebase.」的工作階段使用。
- 常見觸發語句：create design system rules / generate rules for my project / set up design rules / customize design system guidelines
- 常出現在正式施工前的環境準備與工具接線階段。

### 原文精華摘錄
> Generates custom design system rules for the user's codebase
> Use when user says "create design system rules", "generate rules for my project", "set up design rules", "customize design system guidelines", or wants to establish project-specific conventions for Figma-to-code workf...
> Requires Figma MCP server connection
> Overview
> This skill helps you generate custom design system rules tailored to your project's specific needs. These rules guide AI coding agents to produce consistent, high-quality code when implementing Figma designs, ensuring that your team's conventions, component...
> ### Supported Rule Files
> | Agent | Rule File |
> |-------|-----------|
> What Are Design System Rules?
> Design system rules are project-level instructions that encode the "unwritten knowledge" of your codebase - the kind of expertise that experienced developers know and would pass on to new team members:
> - Which layout primitives and components to use
> - Where component files should be located
> - How components should be named and structured
> Prerequisites
> - Figma MCP server must be connected and accessible
> - Access to the project codebase for analysis

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：best, common, guidelines, integration
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：categories, common, component, examples
- programmatic-seo（marketingskills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：common, generate, integration, issues
- design-html（gstack） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：generate, generates, rules, server

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
