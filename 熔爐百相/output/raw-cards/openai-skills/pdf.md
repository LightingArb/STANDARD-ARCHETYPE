---
## openai-skills / pdf

### 來源
- repo：openai-skills
- 路徑：skills/.curated/pdf/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/pdf/skill.md

### 一句話定位
"Use when tasks involve reading, creating, or reviewing PDF files where renderi…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 品味導向

### 核心思考框架
- [Workflow]
- 1. Prefer visual review: render PDF pages to PNGs and inspect them.
- - Use `pdftoppm` if available.
- - If unavailable, install Poppler or ask the user to review the output locally.
- 2. Use `reportlab` to generate PDFs when creating new documents.
- 3. Use `pdfplumber` (or `pypdf`) for text extraction and quick checks; do not rely on it for layout fidelity.
- 4. After each meaningful update, re-render pages and verify alignment, spacing, and legibility.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Do not deliver until the latest PNG inspection shows zero visual or formatting defects.

### 提問方式
無明確提問模板

### 審查維度
- "Use when tasks involve reading, creating, or reviewing PDF files where rendering and layout matter
- prefer visual checks by rendering pages (Poppler) and use Python tools such as `reportlab`, `pdfplumber`, and `pypdf` for generation and extraction."

### 輸出格式要求
- [Temp and output conventions]
- - Use `tmp/pdfs/` for intermediate files; delete when done.
- - Write final artifacts under `output/pdf/` when working in this repo.
- - Keep filenames stable and descriptive.

### 適用場景
- 適合在需要「"Use when tasks involve reading, creating, or reviewing PDF files where renderi…」的工作階段使用。
- 常見觸發語句：Use when tasks involve reading, creating, or reviewing PDF files where rendering and layout matter; prefer visual checks by rendering pages (Poppler) and use Python tools such as `reportlab`, `pdfplumber`, and `pypdf` for generation and extraction.
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> "Use when tasks involve reading, creating, or reviewing PDF files where rendering and layout matter
> prefer visual checks by rendering pages (Poppler) and use Python tools such as `reportlab`, `pdfplumber`, and `pypdf` for generation and extraction."
> When to use
> - Read or review PDF content where layout and visuals matter.
> - Create PDFs programmatically with reliable formatting.
> - Validate final rendering before delivery.
> Workflow
> 1. Prefer visual review: render PDF pages to PNGs and inspect them.
> - Use `pdftoppm` if available.
> - If unavailable, install Poppler or ask the user to review the output locally.

### 和其他 skill 的潛在關聯
- pdf（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：creating, files, pages, pdf
- lead-magnets（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：generation, output, pdf, tools
- programmatic-seo（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：審查型、工具程序型、元技能型；共同關鍵詞：checks, output, pages, quality
- seo-audit（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：審查型、工具程序型、元技能型；共同關鍵詞：output, pages, quality, tools

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
