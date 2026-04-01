---
## anthropics-skills / xlsx

### 來源
- repo：anthropics-skills
- 路徑：skills/xlsx/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：anthropics-skills::skills/xlsx/skill.md

### 一句話定位
"Use this skill any time a spreadsheet file is the primary input or output.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Overview]
- A user may ask you to create, edit, or analyze the contents of an .xlsx file. You have different tools and workflows available for different tasks.
- [Excel File Workflows]
- [Common Workflow]
- 1. **Choose tool**: pandas for data, openpyxl for formulas/formatting
- 2. **Create/Load**: Create new workbook or load existing file
- 3. **Modify**: Add/edit data, formulas, and formatting
- 4. **Save**: Write to file
- 5. **Recalculate formulas (MANDATORY IF USING FORMULAS)**: Use the scripts/recalc.py script
- 6. **Verify and fix any errors**:
- [Formula Verification Checklist]
- Quick checks to ensure formulas work correctly:
- - [ ] **Test 2-3 sample references**: Verify they pull correct values before building full model
- - [ ] **Column mapping**: Confirm Excel columns match (e.g., column 64 = BL, not BK)
- - [ ] **Row offset**: Remember Excel rows are 1-indexed (DataFrame row 5 = Excel row 6)
- - [ ] **NaN handling**: Check for null values with `pd.notna()`
- - [ ] **Far-right columns**: FY data often in columns 50+
- [Code Style Guidelines]

### 核心行為規則
必須做
- - Every Excel model MUST be delivered with ZERO formula errors (#REF!, #DIV/0!, #VALUE!, #N/A, #NAME?)
- - Existing template conventions ALWAYS override these guidelines
- - **Currency**: Use $#,##0 format; ALWAYS specify units in headers ("Revenue ($mm)")

禁止做
- - Never impose standardized formatting on files with established patterns

### 提問方式
無明確提問模板

### 審查維度
- [Formula Verification Checklist]
- Quick checks to ensure formulas work correctly:
- - [ ] **Test 2-3 sample references**: Verify they pull correct values before building full model
- - [ ] **Column mapping**: Confirm Excel columns match (e.g., column 64 = BL, not BK)
- - [ ] **Row offset**: Remember Excel rows are 1-indexed (DataFrame row 5 = Excel row 6)
- - [ ] **NaN handling**: Check for null values with `pd.notna()`
- - [ ] **Far-right columns**: FY data often in columns 50+

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use this skill any time a spreadsheet file is the primary input or output.」的工作階段使用。
- 常見觸發語句：Use this skill any time a spreadsheet file is the primary input or output. This means any task where the user wants to: open, read, edit, or fix an existing .xlsx, .xlsm, .csv, or .tsv file (e.g., adding columns, computing formulas, formatting, charting, cleaning messy data); create a new spreadsheet from scratch or from other data sources; or convert between tabular file formats. Trigger especially when the user references a spreadsheet file by name or path — even casually (like \ / ) — and wants something done to it or produced from it. Also trigger for cleaning or restructuring messy tabular data files (malformed rows, misplaced headers, junk data) into proper spreadsheets. The deliverable must be a spreadsheet file. Do NOT trigger when the primary deliverable is a Word document, HTML report, standalone Python script, database pipeline, or Google Sheets API integration, even if tabular data is involved.

### 原文精華摘錄
> "Use this skill any time a spreadsheet file is the primary input or output
> This means any task where the user wants to: open, read, edit, or fix an existing .xlsx, .xlsm, .csv, or .tsv file (e.g., adding columns, computing formulas, formatting, charting, cleaning messy data)
> create a new spreadsheet from scratch or from other data sources
> or convert between tabular file formats
> Trigger especially when the user references a spreadsheet file by name or path — even casually (like \"the xlsx in my downloads\") — and wants something done to it or produced from it
> Also trigger for cleaning or restructuring messy tabular data files (malformed rows, misplaced headers, junk data) into proper spreadsheets
> The deliverable must be a spreadsheet file
> Do NOT trigger when the primary deliverable is a Word document, HTML report, standalone Python script, database pipeline, or Google Sheets API integration, even if tabular data is involved."
> All Excel files
> - Use a consistent, professional font (e.g., Arial, Times New Roman) for all deliverables unless otherwise instructed by the user

### 和其他 skill 的潛在關聯
- spreadsheet（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：analyzing, csv, existing, formatting
- programmatic-seo（marketingskills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：also, common, data, existing
- seo-audit（marketingskills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：adding, also, common, data
- customer-research（marketingskills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：analyzing, convert, deliverable, done

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
