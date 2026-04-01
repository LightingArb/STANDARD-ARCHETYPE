---
## ui-ux-pro-max-skill / design

### 來源
- repo：ui-ux-pro-max-skill
- 路徑：.claude/skills/design/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：ui-ux-pro-max-skill::.claude/skills/design/skill.md

### 一句話定位
"Comprehensive design skill: brand identity, design tokens, UI styling, logo ge…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Workflows]
- 1. **Logo** → `scripts/logo/generate.py` → Generate logo variants
- 2. **CIP** → `scripts/cip/generate.py --logo ...` → Create deliverable mockups
- 3. **Presentation** → Load `references/slides-create.md` → Build pitch deck
- 1. **Brand** (brand skill) → Define colors, typography, voice
- 2. **Tokens** (design-system skill) → Create semantic token layers
- 3. **Implement** (ui-styling skill) → Configure Tailwind, shadcn/ui

### 核心行為規則
必須做
- **ALWAYS** generate output logo images with white background.
- After generation, **ALWAYS** ask user about HTML preview via `AskUserQuestion`. If yes, invoke `/ui-ux-pro-max` for gallery.

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Comprehensive design skill: brand identity, design tokens, UI styling, logo ge…」的工作階段使用。
- 常見觸發語句：Comprehensive design skill: brand identity, design tokens, UI styling, logo generation (55 styles, Gemini AI), corporate identity program (50 deliverables, CIP mockups), HTML presentations (Chart.js), banner design (22 styles, social/ads/web/print), icon design (15 styles, SVG, Gemini 3.1 Pro), social photos (HTML→screenshot, multi-platform). Actions: design logo, create CIP, generate mockups, build slides, design banner, generate icon, create social photos, social media images, brand identity, design system. Platforms: Facebook, Twitter, LinkedIn, YouTube, Instagram, Pinterest, TikTok, Threads, Google Ads.

### 原文精華摘錄
> "Comprehensive design skill: brand identity, design tokens, UI styling, logo generation (55 styles, Gemini AI), corporate identity program (50 deliverables, CIP mockups), HTML presentations (Chart.js), banner design (...
> Actions: design logo, create CIP, generate mockups, build slides, design banner, generate icon, create social photos, social media images, brand identity, design system
> Platforms: Facebook, Twitter, LinkedIn, YouTube, Instagram, Pinterest, TikTok, Threads, Google Ads."
> When to Use
> - Brand identity, voice, assets
> - Design system tokens and specs
> - UI styling with shadcn/ui + Tailwind
> - Logo design and AI generation
> Sub-skill Routing
> | Task | Sub-skill | Details |

### 和其他 skill 的潛在關聯
- paid-ads（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：ads, facebook, generation, google
- imagegen（openai-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：generate, html, icon, images
- social-content（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：facebook, instagram, linkedin, media
- ad-creative（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：ads, facebook, generate, generation

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [x] 人格型
- [x] 流程型
- [x] 元技能型
---
