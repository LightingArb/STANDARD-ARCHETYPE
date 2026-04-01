---
## openai-skills / speech

### 來源
- repo：openai-skills
- 路徑：skills/.curated/speech/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/speech/skill.md

### 一句話定位
"Use when the user asks for text-to-speech narration or voiceover, accessibilit…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 角色鮮明, 觀點強烈, 規範導向

### 核心思考框架
- [Decision tree (single vs batch)]
- - If the user provides multiple lines/prompts or wants many outputs -> **batch**
- - Else -> **single**
- [Workflow]
- 1. Decide intent: single vs batch (see decision tree above).
- 2. Collect inputs up front: exact text (verbatim), desired voice, delivery style, format, and any constraints.
- 3. If batch: write a temporary JSONL under tmp/ (one job per line), run once, then delete the JSONL.
- 4. Augment instructions into a short labeled spec without rewriting the input text.
- 5. Run the bundled CLI (`scripts/text_to_speech.py`) with sensible defaults (see references/cli.md).
- 6. For important clips, validate: intelligibility, pacing, pronunciation, and adherence to constraints.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Never ask the user to paste the full key in chat. Ask them to set it locally and confirm when ready.
- - Never modify `scripts/text_to_speech.py`. If something is missing, ask the user before doing anything else.
- - Do not introduce a new persona, accent, or emotional style the user did not request.
- - Do not rewrite the input text.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
- [Temp and output conventions]
- - Use `tmp/speech/` for intermediate files (for example JSONL batches); delete when done.
- - Write final artifacts under `output/speech/` when working in this repo.
- - Use `--out` or `--out-dir` to control output paths; keep filenames stable and descriptive.

### 適用場景
- 適合在需要「"Use when the user asks for text-to-speech narration or voiceover, accessibilit…」的工作階段使用。
- 常見觸發語句：Use when the user asks for text-to-speech narration or voiceover, accessibility reads, audio prompts, or batch speech generation via the OpenAI Audio API; run the bundled CLI (`scripts/text_to_speech.py`) with built-in voices and require `OPENAI_API_KEY` for live calls. Custom voice creation is out of scope.
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> "Use when the user asks for text-to-speech narration or voiceover, accessibility reads, audio prompts, or batch speech generation via the OpenAI Audio API
> run the bundled CLI (`scripts/text_to_speech.py`) with built-in voices and require `OPENAI_API_KEY` for live calls
> Custom voice creation is out of scope."
> When to use
> - Generate a single spoken clip from text
> - Generate a batch of prompts (many lines, many files)
> Decision tree (single vs batch)
> - If the user provides multiple lines/prompts or wants many outputs -> **batch**
> - Else -> **single**
> Workflow

### 和其他 skill 的潛在關聯
- design-html（gstack） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：api, live, rules, text
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、工具程序型、人格型、流程型、元技能型；共同關鍵詞：accessibility, examples, output, reference
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：api, asks, defaults, openai
- design-system（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、工具程序型、人格型、元技能型；共同關鍵詞：best, creation, generation, practices

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [x] 人格型
- [x] 流程型
- [x] 元技能型
---
