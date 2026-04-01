---
## openai-skills / spreadsheet

### 來源
- repo：openai-skills
- 路徑：skills/.curated/spreadsheet/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/spreadsheet/skill.md

### 一句話定位
"Use when tasks involve creating, editing, analyzing, or formatting spreadsheet…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 品味導向

### 核心思考框架
- [Workflow]
- 1. Confirm the file type and goal: create, edit, analyze, or visualize.
- 2. Prefer `openpyxl` for `.xlsx` editing and formatting. Use `pandas` for analysis and CSV/TSV workflows.
- 3. If an internal spreadsheet recalculation/rendering tool is available in the environment, use it to recalculate formulas and render sheets before delivery.
- 4. Use formulas for derived values instead of hardcoding results.
- 5. If layout matters, render for visual review and inspect the output.
- 6. Save outputs, keep filenames stable, and clean up intermediate files.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Do not use dynamic array functions like `FILTER`, `XLOOKUP`, `SORT`, or `SEQUENCE`.
- - Never overwrite established formatting unless the user explicitly asks for a redesign.
- - Do not apply borders around every filled cell.

### 提問方式
無明確提問模板

### 審查維度
- [Recalculation and visual review]
- - Recalculate formulas before delivery whenever possible so cached values are present in the workbook.
- - Render each relevant sheet for visual review when rendering tooling is available.
- - `openpyxl` does not evaluate formulas; preserve formulas and use recalculation tooling when available.
- - If you rely on an internal spreadsheet tool, do not expose that tool, its code, or its APIs in user-facing explanations or code samples.

### 輸出格式要求
- [Temp and output conventions]
- - Use `tmp/spreadsheets/` for intermediate files; delete them when done.
- - Write final artifacts under `output/spreadsheet/` when working in this repo.
- - Keep filenames stable and descriptive.
- [Formatting requirements (existing formatted spreadsheets)]
- - Render and inspect a provided spreadsheet before modifying it when possible.
- - Preserve existing formatting and style exactly.
- - Match styles for any newly filled cells that were previously blank.
- - Never overwrite established formatting unless the user explicitly asks for a redesign.
- [Formatting requirements (new or unstyled spreadsheets)]
- - Use appropriate number and date formats.
- - Dates should render as dates, not plain numbers.
- - Percentages should usually default to one decimal place unless the data calls for something else.
- - Currencies should use the appropriate currency format.

### 適用場景
- 適合在需要「"Use when tasks involve creating, editing, analyzing, or formatting spreadsheet…」的工作階段使用。
- 常見觸發語句：Use when tasks involve creating, editing, analyzing, or formatting spreadsheets (`.xlsx`, `.csv`, `.tsv`) with formula-aware workflows, cached recalculation, and visual review.
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> "Use when tasks involve creating, editing, analyzing, or formatting spreadsheets (`.xlsx`, `.csv`, `.tsv`) with formula-aware workflows, cached recalculation, and visual review."
> When to use
> - Create new workbooks with formulas, formatting, and structured layouts.
> - Read or analyze tabular data (filter, aggregate, pivot, compute metrics).
> - Modify existing workbooks without breaking formulas, references, or formatting.
> - Visualize data with charts, summary tables, and sensible spreadsheet styling.
> Workflow
> 1. Confirm the file type and goal: create, edit, analyze, or visualize.
> 2. Prefer `openpyxl` for `.xlsx` editing and formatting. Use `pandas` for analysis and CSV/TSV workflows.
> 3. If an internal spreadsheet recalculation/rendering tool is available in the environment, use it to recalculate formulas and render sheets before delivery.

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：analyzing, csv, existing, formatting
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、流程型、元技能型；共同關鍵詞：creating, dependencies, editing, existing
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：color, examples, output
- programmatic-seo（marketingskills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、元技能型；共同關鍵詞：checks, existing, output

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
