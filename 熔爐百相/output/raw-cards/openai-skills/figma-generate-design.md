---
## openai-skills / figma-generate-design

### 來源
- repo：openai-skills
- 路徑：skills/.curated/figma-generate-design/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/figma-generate-design/skill.md

### 一句話定位
"Use this skill alongside figma-use when the task involves translating an appli…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 品味導向

### 核心思考框架
- [Parallel Workflow with generate_figma_design (Web Apps Only)]
- When building a screen from a **web app** that can be rendered in a browser, the best results come from running both approaches in parallel:
- 1. **In parallel:**
- - Start building the screen using this skill's workflow (use_figma + design system components)
- - Run `generate_figma_design` to capture a pixel-perfect screenshot of the running web app
- 2. **Once both complete:** Update the use_figma output to match the pixel-perfect layout from the `generate_figma_design` capture. The capture provides the exact spacing, sizing, and visual treatment to aim for, while your use_figma output has proper component instances linked to the design system.
- 3. **Once confirmed looking good:** Delete the `generate_figma_design` output — it was only used as a visual reference.
- [Required Workflow]
- **Follow these steps in order. Do not skip steps.**
- Before touching Figma, understand what you're building:
- 1. If building from code, read the relevant source files to understand the page structure, sections, and which components are used.
- 2. Identify the major sections of the screen (e.g., Header, Hero, Content Panels, Pricing Grid, FAQ Accordion, Footer).
- 3. For each section, list the UI components involved (buttons, inputs, cards, navigation pills, accordions, etc.).
- **Preferred: inspect existing screens first.** If the target file already contains screens using the same design system, skip `search_design_system` and inspect existing instances directly. A single `use_figma` call that walks an existing frame's instances gives you an exact, authoritative component map:

### 核心行為規則
必須做
- **MANDATORY**: You MUST also load [figma-use](../figma-use/SKILL.md) before any `use_figma` call. That skill contains critical rules (color ranges, font loading, etc.) that apply to every script you write.
- Before touching Figma, understand what you're building:

禁止做
- **Follow these steps in order. Do not skip steps.**
- You need three things from the design system: **components** (buttons, cards, etc.), **variables** (colors, spacing, radii), and **styles** (text styles, effect styles like shadows). Don't hardcode hex colors or pixel values when design system tokens exist.
- > **Never conclude "no variables exist" based solely on `getLocalVariableCollectionsAsync()` returning empty.** Always also run `search_design_system` with `includeVariables: true` to check for library variables before deciding to create your own.
- **Never hardcode hex colors or pixel spacing** when a design system variable exists. Use `setBoundVariable` for spacing/radii and `setBoundVariableForPaint` for colors. Apply text styles with `node.textStyleId` and effect styles with `node.effectStyleId`.
- After composing all sections, call `get_screenshot` on the full page frame and compare against the source. Fix any issues with targeted `use_figma` calls — don't rebuild the entire screen.
- - **Work section by section.** Never build more than one major section per `use_figma` call.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use this skill alongside figma-use when the task involves translating an appli…」的工作階段使用。
- 常見觸發語句：Use this skill alongside figma-use when the task involves translating an application page, view, or multi-section layout into Figma. Triggers: 'write to Figma', 'create in Figma from code', 'push page to Figma', 'take this app/page and build it in Figma', 'create a screen', 'build a landing page in Figma', 'update the Figma screen to match code'. This is the preferred workflow skill whenever the user wants to build or update a full page, screen, or view in Figma from code or a description. Discovers design system components, variables, and styles via search_design_system, imports them, and assembles screens incrementally section-by-section using design system tokens instead of hardcoded values.
- 常出現在正式施工前的環境準備與工具接線階段。

### 原文精華摘錄
> "Use this skill alongside figma-use when the task involves translating an application page, view, or multi-section layout into Figma
> Triggers: 'write to Figma', 'create in Figma from code', 'push page to Figma', 'take this app/page and build it in Figma', 'create a screen', 'build a landing page in Figma', 'update the Figma screen to match code'
> This is the preferred workflow skill whenever the user wants to build or update a full page, screen, or view in Figma from code or a description
> Discovers design system components, variables, and styles via search_design_system, imports them, and assembles screens incrementally section-by-section using design system tokens instead of hardcoded values."
> Skill Boundaries
> - Use this skill when the deliverable is a **Figma screen** (new or updated) composed of design system component instances.
> - If the user wants to generate **code from a Figma design**, switch to [figma-implement-design](../figma-implement-design/SKILL.md).
> - If the user wants to create **new reusable components or variants**, use [figma-use](../figma-use/SKILL.md) directly.
> - If the user wants to write **Code Connect mappings**, switch to [figma-code-connect-components](../figma-code-connect-components/SKILL.md).
> Prerequisites

### 和其他 skill 的潛在關聯
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：app, build, landing, layout
- ad-creative（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：full, generate, landing, page
- copywriting（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：best, landing, page, practices
- docx（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：docs, page, reference, triggers

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
