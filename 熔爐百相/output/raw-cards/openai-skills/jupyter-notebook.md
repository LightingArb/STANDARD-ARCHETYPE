---
## openai-skills / jupyter-notebook

### 來源
- repo：openai-skills
- 路徑：skills/.curated/jupyter-notebook/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/jupyter-notebook/skill.md

### 一句話定位
"Use when the user asks to create, scaffold, or edit Jupyter notebooks (.ipynb)…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 規範導向, 元認知

### 核心思考框架
- [Decision tree]
- - If the request is exploratory, analytical, or hypothesis-driven, choose `experiment`.
- - If the request is instructional, step-by-step, or audience-specific, choose `tutorial`.
- - If editing an existing notebook, treat it as a refactor: preserve intent and improve structure.
- [Workflow]
- 1. Lock the intent.
- 2. Scaffold from the template.
- 3. Fill the notebook with small, runnable steps.
- 4. Apply the right pattern.
- 5. Edit safely when working with existing notebooks.
- 6. Validate the result.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
- [Templates and helper script]
- - Templates live in `assets/experiment-template.ipynb` and `assets/tutorial-template.ipynb`.
- - The helper script loads a template, updates the title cell, and writes a notebook.
- Script path:
- - `$JUPYTER_NOTEBOOK_CLI` (installed default: `$CODEX_HOME/skills/jupyter-notebook/scripts/new_notebook.py`)
- [Temp and output conventions]
- - Use `tmp/jupyter-notebook/` for intermediate files; delete when done.
- - Write final artifacts under `output/jupyter-notebook/` when working in this repo.
- - Use stable, descriptive filenames (for example, `ablation-temperature.ipynb`).

### 適用場景
- 適合在需要「"Use when the user asks to create, scaffold, or edit Jupyter notebooks (.ipynb)…」的工作階段使用。
- 常見觸發語句：Use when the user asks to create, scaffold, or edit Jupyter notebooks (`.ipynb`) for experiments, explorations, or tutorials; prefer the bundled templates and run the helper script `new_notebook.py` to generate a clean starting notebook.

### 原文精華摘錄
> "Use when the user asks to create, scaffold, or edit Jupyter notebooks (`.ipynb`) for experiments, explorations, or tutorials
> prefer the bundled templates and run the helper script `new_notebook.py` to generate a clean starting notebook."
> When to use
> - Create a new `.ipynb` notebook from scratch.
> - Convert rough notes or scripts into a structured notebook.
> - Refactor an existing notebook to be more reproducible and skimmable.
> - Build experiments or tutorials that will be read or re-run by other people.
> Decision tree
> - If the request is exploratory, analytical, or hypothesis-driven, choose `experiment`.
> - If the request is instructional, step-by-step, or audience-specific, choose `tutorial`.

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、流程型、元技能型；共同關鍵詞：edit, new, output, path
- docx（anthropics-skills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、流程型、元技能型；共同關鍵詞：asks, dependencies, edit, new
- analytics-tracking（marketingskills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、流程型、元技能型；共同關鍵詞：asks, conventions, output, set
- marketing-ideas（marketingskills） - 相似 - 共享領域：meta, shipping；共享分類：思考框架型、審查型、流程型、元技能型；共同關鍵詞：asks, output, reference, starting

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
