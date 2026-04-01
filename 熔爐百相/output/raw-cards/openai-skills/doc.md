---
## openai-skills / doc

### 來源
- repo：openai-skills
- 路徑：skills/.curated/doc/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/doc/skill.md

### 一句話定位
"Use when the task involves reading, creating, or editing .docx documents, espe…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 品味導向

### 核心思考框架
- [Workflow]
- 1. Prefer visual review (layout, tables, diagrams).
- - If `soffice` and `pdftoppm` are available, convert DOCX -> PDF -> PNGs.
- - Or use `scripts/render_docx.py` (requires `pdf2image` and Poppler).
- - If these tools are missing, install them or ask the user to review rendered pages locally.
- 2. Use `python-docx` for edits and structured creation (headings, styles, tables, lists).
- 3. After each meaningful change, re-render and inspect the pages.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
- prefer `python-docx` plus the bundled `scripts/render_docx.py` for visual checks."

### 輸出格式要求
- [Temp and output conventions]
- - Use `tmp/docs/` for intermediate files; delete when done.
- - Write final artifacts under `output/doc/` when working in this repo.
- - Keep filenames stable and descriptive.

### 適用場景
- 適合在需要「"Use when the task involves reading, creating, or editing .docx documents, espe…」的工作階段使用。
- 常見觸發語句：Use when the task involves reading, creating, or editing `.docx` documents, especially when formatting or layout fidelity matters; prefer `python-docx` plus the bundled `scripts/render_docx.py` for visual checks.

### 原文精華摘錄
> "Use when the task involves reading, creating, or editing `.docx` documents, especially when formatting or layout fidelity matters
> prefer `python-docx` plus the bundled `scripts/render_docx.py` for visual checks."
> When to use
> - Read or review DOCX content where layout matters (tables, diagrams, pagination).
> - Create or edit DOCX files with professional formatting.
> - Validate visual layout before delivery.
> Workflow
> 1. Prefer visual review (layout, tables, diagrams).
> - If `soffice` and `pdftoppm` are available, convert DOCX -> PDF -> PNGs.
> - Or use `scripts/render_docx.py` (requires `pdf2image` and Poppler).

### 和其他 skill 的潛在關聯
- docx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：creating, dependencies, doc, documents
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：especially, formatting, output, reading
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、流程型、元技能型；共同關鍵詞：creating, dependencies, editing, output
- doc-coauthoring（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：creating, doc, final

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
