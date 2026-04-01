---
## openai-skills / transcribe

### 來源
- repo：openai-skills
- 路徑：skills/.curated/transcribe/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/transcribe/skill.md

### 一句話定位
"Transcribe audio files to text with optional diarization and known-speaker hin…

### 核心人格特質
流程紀律, 完成導向, 規範導向, 元認知

### 核心思考框架
- [Workflow]
- 1. Collect inputs: audio file path(s), desired response format (text/json/diarized_json), optional language hint, and any known speaker references.
- 2. Verify `OPENAI_API_KEY` is set. If missing, ask the user to set it locally (do not ask them to paste the key).
- 3. Run the bundled `transcribe_diarize.py` CLI with sensible defaults (fast text transcription).
- 4. Validate the output: transcription quality, speaker labels, and segment boundaries; iterate with a single targeted change if needed.
- 5. Save outputs under `output/transcribe/` when working in this repo.
- [Decision rules]
- - Default to `gpt-4o-mini-transcribe` with `--response-format text` for fast transcription.
- - If the user wants speaker labels or diarization, use `--model gpt-4o-transcribe-diarize --response-format diarized_json`.
- - If audio is longer than ~30 seconds, keep `--chunking-strategy auto`.
- - Prompting is not supported for `gpt-4o-transcribe-diarize`.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Never ask the user to paste the full key in chat.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
- [Output conventions]
- - Use `output/transcribe/<job-id>/` for evaluation runs.
- - Use `--out-dir` for multiple files to avoid overwriting.

### 適用場景
- 適合在需要「"Transcribe audio files to text with optional diarization and known-speaker hin…」的工作階段使用。
- 常見觸發語句：Transcribe audio files to text with optional diarization and known-speaker hints. Use when a user asks to transcribe speech from audio/video, extract text from recordings, or label speakers in interviews or meetings.

### 原文精華摘錄
> "Transcribe audio files to text with optional diarization and known-speaker hints
> Use when a user asks to transcribe speech from audio/video, extract text from recordings, or label speakers in interviews or meetings."
> Workflow
> 1. Collect inputs: audio file path(s), desired response format (text/json/diarized_json), optional language hint, and any known speaker references.
> 2. Verify `OPENAI_API_KEY` is set. If missing, ask the user to set it locally (do not ask them to paste the key).
> 3. Run the bundled `transcribe_diarize.py` CLI with sensible defaults (fast text transcription).
> 4. Validate the output: transcription quality, speaker labels, and segment boundaries; iterate with a single targeted change if needed.
> Decision rules
> - Default to `gpt-4o-mini-transcribe` with `--response-format text` for fast transcription.
> - If the user wants speaker labels or diarization, use `--model gpt-4o-transcribe-diarize --response-format diarized_json`.

### 和其他 skill 的潛在關聯
- pdf（anthropics-skills） - 相似 - 共享領域：design, meta；共享分類：流程型、元技能型；共同關鍵詞：asks, files, quick, reference
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta；共享分類：流程型、元技能型；共同關鍵詞：dependencies, files, output, quick
- docx（anthropics-skills） - 相似 - 共享領域：design, meta；共享分類：流程型、元技能型；共同關鍵詞：asks, dependencies, files, quick
- marketing-ideas（marketingskills） - 相似 - 共享領域：design, meta；共享分類：流程型、元技能型；共同關鍵詞：asks, output, quick, reference

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
