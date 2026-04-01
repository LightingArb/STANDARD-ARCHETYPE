---
## openai-skills / sora

### 來源
- repo：openai-skills
- 路徑：skills/.curated/sora/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/sora/skill.md

### 一句話定位
"Use when the user asks to generate, edit, extend, poll, list, download, or del…

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 角色鮮明, 觀點強烈, 規範導向, 元認知

### 核心思考框架
- [Decision tree]
- - If the user has a short non-human reference clip they want to reuse across shots → `create-character`
- - If the user has a completed video and wants the next beat/continuation → `extend`
- - If the user has a completed video and wants a targeted change while preserving the shot → `edit`
- - If the user has a video id and wants status or assets → `status`, `poll`, or `download`
- - If the user needs many renders immediately inside Codex → `create-batch` (local fan-out, not the Batch API)
- - If the user needs many renders for offline processing or a studio pipeline → use the official Batch API flow described in `references/video-api.md`
- [Workflow]
- 1. Decide intent: create vs create-character vs edit vs extend vs status/download vs local queue vs official Batch API.
- 2. Collect inputs: prompt, model, size, seconds, any image reference, and any character IDs.
- 3. Prefer CLI augmentation flags (`--use-case`, `--scene`, `--camera`, etc.) instead of hand-writing a long structured prompt. If you already have a structured prompt file, pass `--no-augment`.
- 4. Run the bundled CLI (`scripts/sora.py`) with sensible defaults. For long prompts, prefer `--prompt-file` to avoid shell-escaping issues.
- 5. For async jobs, poll until terminal status (or use `create-and-poll`).
- 6. Download assets (video/thumbnail/spritesheet) and save them locally before URLs expire.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Never ask the user to paste the full key in chat. Ask them to set it locally and confirm when ready.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Use when the user asks to generate, edit, extend, poll, list, download, or del…」的工作階段使用。
- 常見觸發語句：Use when the user asks to generate, edit, extend, poll, list, download, or delete Sora videos, create reusable non-human Sora character references, or run local multi-video queues via the bundled CLI (`scripts/sora.py`); includes requests like: (i) generate AI video, (ii) edit this Sora clip, (iii) extend this video, (iv) create a character reference, (v) download video/thumbnail/spritesheet, and (vi) Sora batch planning; requires `OPENAI_API_KEY` and Sora API access.
- 常出現在規劃、定義問題、確認範圍或決策前討論階段。

### 原文精華摘錄
> "Use when the user asks to generate, edit, extend, poll, list, download, or delete Sora videos, create reusable non-human Sora character references, or run local multi-video queues via the bundled CLI (`scripts/sora.py`)
> includes requests like: (i) generate AI video, (ii) edit this Sora clip, (iii) extend this video, (iv) create a character reference, (v) download video/thumbnail/spritesheet, and (vi) Sora batch planning
> requires `OPENAI_API_KEY` and Sora API access."
> When to use
> - Generate a new video clip from a prompt
> - Create a reusable character reference from a short non-human source clip
> - Edit an existing generated video with a targeted prompt change
> - Extend a completed video with a continuation prompt
> Decision tree
> - If the user has a short non-human reference clip they want to reuse across shots → `create-character`

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：api, best, edit, like
- claude-api（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：api, asks, defaults, openai
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：流程型、元技能型；共同關鍵詞：includes, like, notes, reference
- design-html（gstack） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、人格型、流程型、元技能型；共同關鍵詞：api, generate, rules, type

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [x] 人格型
- [x] 流程型
- [x] 元技能型
---
