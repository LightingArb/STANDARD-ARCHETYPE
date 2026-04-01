---
## openai-skills / imagegen

### 來源
- repo：openai-skills
- 路徑：skills/.curated/imagegen/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/imagegen/skill.md

### 一句話定位
"Generate or edit raster images when the task benefits from AI-created bitmap v…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Top-level modes and rules]
- This skill has exactly two top-level modes:
- - **Default built-in tool mode (preferred):** built-in `image_gen` tool for normal image generation and editing. Does not require `OPENAI_API_KEY`.
- - **Fallback CLI mode (explicit-only):** `scripts/image_gen.py` CLI. Use only when the user explicitly asks for the CLI path. Requires `OPENAI_API_KEY`.
- Within the explicit CLI fallback only, the CLI exposes three subcommands:
- - `generate`
- - `edit`
- [Decision tree]
- Think about two separate questions:
- 1. **Intent:** is this a new image or an edit of an existing image?
- 2. **Execution strategy:** is this one asset or many assets/variants?
- Intent:
- - If the user wants to modify an existing image while preserving parts of it, treat the request as **edit**.
- - If the user provides images only as references for style, composition, mood, or subject guidance, treat the request as **generate**.
- [Workflow]
- 1. Decide the top-level mode: built-in by default, fallback CLI only if explicitly requested.
- 2. Decide the intent: `generate` or `edit`.
- 3. Decide whether the output is preview-only or meant to be consumed by the current project.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Never switch to CLI fallback automatically.
- - If the user explicitly asks for CLI mode, use the bundled `scripts/image_gen.py` workflow. Do not create one-off SDK runners.
- - Never modify `scripts/image_gen.py`. If something is missing, ask the user before doing anything else.
- - Do not describe or rely on OS temp as the default built-in destination.
- - Do not describe or rely on a destination-path argument (if any) on the built-in `image_gen` tool. If a specific location is needed, generate first and then move or copy the selected output from `$CODEX_HOME/generated_images/...`.
- - Never leave a project-referenced asset only at the default `$CODEX_HOME/*` path.
- - Do not overwrite an existing asset unless the user explicitly asked for replacement; otherwise create a sibling versioned filename such as `hero-v2.png` or `item-icon-edited.png`.
- - Do not promise arbitrary filesystem-path editing through the built-in tool.
- 15. For project-bound work, move or copy the selected artifact into the workspace and update any consuming code or references. Never leave a project-referenced asset only at the default `$CODEX_HOME/generated_images/...` path.
- - Fallback-only execution notes such as `Quality:`, `Input fidelity:`, masks, output format, and output paths belong in the explicit CLI path only. Do not treat them as built-in `image_gen` tool arguments.
- - Do not ask the user for `OPENAI_API_KEY` when using the built-in `image_gen` tool.
- - Never ask the user to paste the full key in chat. Ask them to set it locally and confirm when ready.

### 提問方式
- 1. **Intent:** is this a new image or an edit of an existing image?
- 2. **Execution strategy:** is this one asset or many assets/variants?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Generate or edit raster images when the task benefits from AI-created bitmap v…」的工作階段使用。
- 常見觸發語句：Generate or edit raster images when the task benefits from AI-created bitmap visuals such as photos, illustrations, textures, sprites, mockups, or transparent-background cutouts. Use when Codex should create a brand-new image, transform an existing image, or derive visual variants from references, and the output should be a bitmap asset rather than repo-native code or vector. Do not use when the task is better handled by editing existing SVG/vector/code-native assets, extending an established icon or logo system, or building the visual directly in HTML/CSS/canvas.

### 原文精華摘錄
> "Generate or edit raster images when the task benefits from AI-created bitmap visuals such as photos, illustrations, textures, sprites, mockups, or transparent-background cutouts
> Use when Codex should create a brand-new image, transform an existing image, or derive visual variants from references, and the output should be a bitmap asset rather than repo-native code or vector
> Do not use when the task is better handled by editing existing SVG/vector/code-native assets, extending an established icon or logo system, or building the visual directly in HTML/CSS/canvas."
> Top-level modes and rules
> This skill has exactly two top-level modes:
> - **Default built-in tool mode (preferred):** built-in `image_gen` tool for normal image generation and editing. Does not require `OPENAI_API_KEY`.
> - **Fallback CLI mode (explicit-only):** `scripts/image_gen.py` CLI. Use only when the user explicitly asks for the CLI path. Requires `OPENAI_API_KEY`.
> Within the explicit CLI fallback only, the CLI exposes three subcommands:
> When to use
> - Generate a new image (concept art, product shot, cover, website hero)

### 和其他 skill 的潛在關聯
- design（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：generate, html, icon, images
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：best, edit, existing, html
- copy-editing（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：better, edit, editing, existing
- copywriting（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：best, editing, existing, guidance

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
