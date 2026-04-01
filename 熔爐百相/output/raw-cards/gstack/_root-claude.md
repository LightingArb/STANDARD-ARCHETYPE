---
## gstack / CLAUDE

### 來源
- repo：gstack
- 路徑：CLAUDE.md
- 檔案類型：CLAUDE.md
- card_kind：overview
- language：en
- canonical_group：gstack::claude.md

### 一句話定位
gstack 倉庫的工程規範、文件生成與提交流程總指引。

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 證據導向, 懷疑論

### 核心思考框架
- [SKILL.md workflow]
- SKILL.md files are **generated** from `.tmpl` templates. To update docs:
- 1. Edit the `.tmpl` file (e.g. `SKILL.md.tmpl` or `browse/SKILL.md.tmpl`)
- 2. Run `bun run gen:skill-docs` (or `bun run build` which does it automatically)
- 3. Commit both the `.tmpl` and generated `.md` files
- **Merge conflicts on SKILL.md files:** NEVER resolve conflicts on generated SKILL.md

### 核心行為規則
必須做
- Before designing any solution that involves concurrency, unfamiliar patterns,
- ├── ETHOS.md # Builder philosophy (Boil the Lake, Search Before Building)

禁止做
- ├── SKILL.md # Generated from SKILL.md.tmpl (don't edit directly)
- **Merge conflicts on SKILL.md files:** NEVER resolve conflicts on generated SKILL.md
- Skills must NEVER hardcode framework-specific commands, file patterns, or directory
- - **Use natural language for logic and state.** Don't use shell variables to pass
- - **Don't hardcode branch names.** Detect `main`/`master`/etc dynamically via
- `/browse` skill or run the browse binary directly via `$B <command>`. NEVER use
- ## Compiled binaries — NEVER commit browse/dist/ or design/dist/
- **NEVER stage or commit these files.** They show up as modified in `git status`
- - Never fold new work into an existing CHANGELOG entry from a prior version that
- bump to v0.10.1.0 with a new entry — don't edit the v0.10.0.0 entry.
- features, bump to v0.13.9.0 with a new entry. Never jam your changes into an entry that
- - **Never mention TODOS.md, internal tracking, eval infrastructure, or contributor-facing

### 提問方式
- 1. What branch am I on? What did THIS branch change?
- - Does CHANGELOG have your branch's own entry separate from main's entries?
- - Is VERSION higher than main's VERSION?
- - Is your entry the topmost entry in CHANGELOG (above main's latest)?

### 審查維度
非審查型

### 輸出格式要求
- [Writing SKILL templates]
- Rules:
- - **Use natural language for logic and state.** Don't use shell variables to pass
- - **Don't hardcode branch names.** Detect `main`/`master`/etc dynamically via
- - **Keep bash blocks self-contained.** Each code block should work independently.
- - **Express conditionals as English.** Instead of nested `if/elif/else` in bash,

### 適用場景
- 適合在需要「gstack 倉庫的工程規範、文件生成與提交流程總指引」的工作階段使用。

### 原文精華摘錄
> `test:evals` requires `ANTHROPIC_API_KEY`
> Codex E2E tests (`test/codex-e2e.test.ts`) use Codex's own auth from `~/.codex/` config — no `OPENAI_API_KEY` env var needed
> E2E tests stream progress in real-time (tool-by-tool via `--output-format stream-json
> Commands
> **Diff-based test selection:** `test:evals` and `test:e2e` auto-select tests based
> **Two-tier system:** Tests are classified as `gate` or `periodic` in `E2E_TIERS`
> `EVALS_TIER=periodic` to filter. When adding new E2E tests, classify them:
> 1. Safety guardrail or deterministic functional test? -> `gate`
> Testing
> `bun test` runs skill validation, gen-skill-docs quality checks, and browse

### 和其他 skill 的潛在關聯
- sora（openai-skills） - 互補 - 共享領域：meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：api, guardrails, key, local
- claude-api（anthropics-skills） - 互補 - 共享領域：meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：anthropic, api, effort, openai
- skill-creator（anthropics-skills） - 互補 - 共享領域：meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：commit, evals, key, structure
- AGENTS（marketingskills） - 互補 - 共享領域：meta；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：commands, structure, style, test

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
