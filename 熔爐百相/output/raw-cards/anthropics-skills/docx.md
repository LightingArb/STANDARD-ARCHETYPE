---
## anthropics-skills / docx

### 來源
- repo：anthropics-skills
- 路徑：skills/docx/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：anthropics-skills::skills/docx/skill.md

### 一句話定位
"Use this skill whenever the user wants to create, read, edit, or manipulate Wo…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
- [Overview]
- A .docx file is a ZIP archive containing XML files.

### 核心行為規則
必須做
- // CRITICAL: type parameter is REQUIRED

禁止做
- ### Lists (NEVER use unicode bullets)
- - **Never use `\n`** - use separate Paragraph elements
- - **Never use unicode bullets** - use `LevelFormat.BULLET` with numbering config
- - **Never use tables as dividers/rules** - cells have minimum height and render as empty boxes (including in headers/footers); use `border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "2E75B6", space: 1 } }` on a Paragraph instead. For two-column footers, use tab stops (see Tab Stops section), not tables
- **Use the Edit tool directly for string replacement. Do not write Python scripts.** Scripts introduce unnecessary complexity. The Edit tool shows exactly what is being replaced.
- - **Replace entire `<w:r>` elements**: When adding tracked changes, replace the whole `<w:r>...</w:r>` block with `<w:del>...<w:ins>...` as siblings. Don't inject tracked change tags inside a run.
- **Restoring another author's deletion** - add insertion after (don't modify their deletion):

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use this skill whenever the user wants to create, read, edit, or manipulate Wo…」的工作階段使用。
- 常見觸發語句：Use this skill whenever the user wants to create, read, edit, or manipulate Word documents (.docx files). Triggers include: any mention of 'Word doc', 'word document', '.docx', or requests to produce professional documents with formatting like tables of contents, headings, page numbers, or letterheads. Also use when extracting or reorganizing content from .docx files, inserting or replacing images in documents, performing find-and-replace in Word files, working with tracked changes or comments, or converting content into a polished Word document. If the user asks for a 'report', 'memo', 'letter', 'template', or similar deliverable as a Word or .docx file, use this skill. Do NOT use for PDFs, spreadsheets, Google Docs, or general coding tasks unrelated to document generation.

### 原文精華摘錄
> "Use this skill whenever the user wants to create, read, edit, or manipulate Word documents (.docx files)
> Triggers include: any mention of 'Word doc', 'word document', '.docx', or requests to produce professional documents with formatting like tables of contents, headings, page numbers, or letterheads
> Also use when extracting or reorganizing content from .docx files, inserting or replacing images in documents, performing find-and-replace in Word files, working with tracked changes or comments, or converting content...
> If the user asks for a 'report', 'memo', 'letter', 'template', or similar deliverable as a Word or .docx file, use this skill
> Do NOT use for PDFs, spreadsheets, Google Docs, or general coding tasks unrelated to document generation."
> Overview
> A .docx file is a ZIP archive containing XML files.
> Quick Reference
> Legacy `.doc` files must be converted before editing:
> To produce a clean document with all tracked changes accepted (requires LibreOffice):
> Creating New Documents
> **Common page sizes (DXA units, 1440 DXA = 1 inch):**
> **Landscape orientation:** docx-js swaps width/height internally, so pass portrait dimensions and let it handle the swap:
> ### Lists (NEVER use unicode bullets)
> **CRITICAL: Tables need dual widths** - set both `columnWidths` on the table AND `width` on each cell. Without both, tables render incorrectly on some platforms.
> Editing Existing Documents

### 和其他 skill 的潛在關聯
- ad-creative（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：also, existing, generation, google
- figma-use（openai-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：creating, docs, edit, file
- programmatic-seo（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、工具程序型、元技能型；共同關鍵詞：also, content, existing, overview
- slides（openai-skills） - 相似 - 共享領域：browser, design, meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：content, edit, existing, new

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
