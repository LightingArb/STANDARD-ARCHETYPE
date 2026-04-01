---
## openai-skills / slides

### 來源
- repo：openai-skills
- 路徑：skills/.curated/slides/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/slides/skill.md

### 一句話定位
Create and edit presentation slide decks (.pptx) with PptxGenJS, bundled layout…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 品味導向

### 核心思考框架
- [Overview]
- Use PptxGenJS for slide authoring. Do not use `python-pptx` for deck generation unless the task is inspection-only; keep editable output in JavaScript and deliver both the `.pptx` and the source `.js`.
- Keep work in a task-local directory. Only copy final artifacts to the requested destination after rendering and validation pass.
- [Workflow]
- 1. Inspect the request and determine whether you are creating a new deck, recreating an existing deck, or editing one.
- 2. Set the slide size up front. Default to 16:9 (`LAYOUT_WIDE`) unless the source material clearly uses another aspect ratio.
- 3. Copy `assets/pptxgenjs_helpers/` into the working directory and import the helpers from there.
- 4. Build the deck in JavaScript with an explicit theme font, stable spacing, and editable PowerPoint-native elements when practical.
- 5. Run the bundled scripts from this skill directory or copy the needed ones into the task workspace. Render the result with `render_slides.py`, review the PNGs, and fix layout issues before delivery.
- 6. Run `slides_test.py` for overflow checks when slide edges are tight or the deck is dense.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- Use PptxGenJS for slide authoring. Do not use `python-pptx` for deck generation unless the task is inspection-only; keep editable output in JavaScript and deliver both the `.pptx` and the source `.js`.
- - Set theme fonts explicitly. Do not rely on PowerPoint defaults if typography matters.
- - For charts or diagrams that PptxGenJS cannot express well, render SVG externally and place the SVG in the slide.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Create and edit presentation slide decks (.pptx) with PptxGenJS, bundled layout…」的工作階段使用。

### 原文精華摘錄
> Create and edit presentation slide decks (`.pptx`) with PptxGenJS, bundled layout helpers, and render/validation utilities
> Use when tasks involve building a new PowerPoint deck, recreating slides from screenshots/PDFs/reference decks, modifying slide content while preserving editable output, adding charts/diagrams/visuals, or diagnosing l...
> Overview
> Use PptxGenJS for slide authoring. Do not use `python-pptx` for deck generation unless the task is inspection-only; keep editable output in JavaScript and deliver both the `.pptx` and the source `.js`.
> Keep work in a task-local directory. Only copy final artifacts to the requested destination after rendering and validation pass.
> Bundled Resources
> - `assets/pptxgenjs_helpers/`: Copy this folder into the deck workspace and import it locally instead of reimplementing helper logic.
> - `scripts/render_slides.py`: Rasterize a `.pptx` or `.pdf` to per-slide PNGs.
> - `scripts/slides_test.py`: Detect content that overflows the slide canvas.
> - `scripts/create_montage.py`: Build a contact-sheet style montage of rendered slides.

### 和其他 skill 的潛在關聯
- docx（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：content, edit, existing, new
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、流程型、元技能型；共同關鍵詞：content, deck, decks, existing
- pdf（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：adding, new, overview, pdfs
- programmatic-seo（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：審查型、工具程序型、元技能型；共同關鍵詞：building, content, existing, issues

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
