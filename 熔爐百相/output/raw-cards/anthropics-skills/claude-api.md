---
## anthropics-skills / claude-api

### 來源
- repo：anthropics-skills
- 路徑：skills/claude-api/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：anthropics-skills::skills/claude-api/skill.md

### 一句話定位
"Build apps with the Claude API or Anthropic SDK.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Reading Guide]
- After detecting the language, read the relevant files based on what the user needs:
- **Single text classification/summarization/extraction/Q&A:**
- **Chat UI or real-time response display:**
- **Long-running conversations (may exceed context window):**
- **Prompt caching / optimize caching / "why is my cache hit rate low":**
- **Function calling / tool use / agents:**

### 核心行為規則
必須做
- Before reading code examples, determine which language the user is working in:
- Before choosing the agent tier, check all four criteria:
- **ALWAYS use `claude-opus-4-6` unless the user explicitly names a different model.** This is non-negotiable. Do not use `claude-sonnet-4-6`, `claude-sonnet-4-5`, or any other model unless the user literally says "use sonnet" or "use haiku". Never downgrade for cost — that's the user's decision, not yours.
- - Use AskUserQuestion with options: Python, TypeScript, Java, Go, Ruby, cURL/raw HTTP, C#, PHP

禁止做
- For the Claude model version, please use Claude Opus 4.6, which you can access via the exact model string `claude-opus-4-6`. Please default to using adaptive thinking (`thinking: {type: "adaptive"}`) for anything remotely complicated. And finally, please default to streaming for any request that may involve long input, long output, or high `max_tokens` — it prevents hitting request timeouts. Use the SDK's `.get_final_message()` / `.finalMessage()` helper to get the complete response if you don't need to handle individual stream events
- └── Yes → Agent SDK — built-in tools, don't reimplement them
- **ALWAYS use `claude-opus-4-6` unless the user explicitly names a different model.** This is non-negotiable. Do not use `claude-sonnet-4-6`, `claude-sonnet-4-5`, or any other model unless the user literally says "use sonnet" or "use haiku". Never downgrade for cost — that's the user's decision, not yours.
- **CRITICAL: Use only the exact model ID strings from the table above — they are complete as-is. Do not append date suffixes.** For example, use `claude-sonnet-4-5`, never `claude-sonnet-4-5-20250514` or any other date-suffixed variant you might recall from training data. If the user requests an older model not in the table (e.g., "opus 4.5", "sonnet 3.7"), read `shared/models.md` for the exact ID — do not construct one yourself.
- **Older models (only if explicitly requested):** If the user specifically asks for Sonnet 4.5 or another older model, use `thinking: {type: "enabled", budget_tokens: N}`. `budget_tokens` must be less than `max_tokens` (minimum 1024). Never choose an older model just because the user mentions `budget_tokens` — use Opus 4.6 with adaptive thinking instead.
- **Top-level auto-caching** (`cache_control: {type: "ephemeral"}` on `messages.create()`) is the simplest option when you don't need fine-grained placement. Max 4 breakpoints per request. Minimum cacheable prefix is ~1024 tokens — shorter prefixes silently won't cache.
- - Don't truncate inputs when passing files or content to the API. If the content is too long to fit in the context window, notify the user and discuss options (chunking, summarization, etc.) rather than silently truncating.
- - **`max_tokens` defaults:** Don't lowball `max_tokens` — hitting the cap truncates output mid-thought and requires a retry. For non-streaming requests, default to `~16000` (keeps responses under SDK HTTP timeouts). For streaming requests, default to `~64000` (timeouts aren't a concern, so give the model room). Only go lower when you have a hard reason: classification (`~256`), cost caps, or deliberately short outputs.
- - **Don't reimplement SDK functionality:** The SDK provides high-level helpers — use them instead of building from scratch. Specifically: use `stream.finalMessage()` instead of wrapping `.on()` events in `new Promise()`; use typed exception classes (`Anthropic.RateLimitError`, etc.) instead of string-matching error messages; use SDK types (`Anthropic.MessageParam`, `Anthropic.Tool`, `Anthropic.Message`, etc.) instead of redefining equivalent interfaces.
- - **Don't define custom types for SDK data structures:** The SDK exports types for all API objects. Use `Anthropic.MessageParam` for messages, `Anthropic.Tool` for tool definitions, `Anthropic.ToolUseBlock` / `Anthropic.ToolResultBlockParam` for tool results, `Anthropic.Message` for responses. Defining your own `interface ChatMessage { role: string; content: unknown }` duplicates what the SDK already provides and loses type safety.

### 提問方式
- ### Should I Build an Agent?
- - **Value** — Does the outcome justify higher cost and latency?
- - **Viability** — Is Claude capable at this task type?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Build apps with the Claude API or Anthropic SDK.」的工作階段使用。
- 常見觸發語句：Build apps with the Claude API or Anthropic SDK. TRIGGER when: code imports `anthropic`/`@anthropic-ai/sdk`/`claude_agent_sdk`, or user asks to use Claude API, Anthropic SDKs, or Agent SDK. DO NOT TRIGGER when: code imports `openai`/other AI SDK, general programming, or ML/data-science tasks.
- 常出現在正式施工前的環境準備與工具接線階段。

### 原文精華摘錄
> "Build apps with the Claude API or Anthropic SDK
> TRIGGER when: code imports `anthropic`/`@anthropic-ai/sdk`/`claude_agent_sdk`, or user asks to use Claude API, Anthropic SDKs, or Agent SDK
> DO NOT TRIGGER when: code imports `openai`/other AI SDK, general programming, or ML/data-science tasks."
> Defaults
> Unless the user requests otherwise:
> Language Detection
> Before reading code examples, determine which language the user is working in:
> 1. **Look at project files** to infer the language:
> - `*.py`, `requirements.txt`, `pyproject.toml`, `setup.py`, `Pipfile` → **Python** — read from `python/`
> - `*.ts`, `*.tsx`, `package.json`, `tsconfig.json` → **TypeScript** — read from `typescript/`

### 和其他 skill 的潛在關聯
- figma-generate-library（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：api, architecture, build, reference
- sora（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：工具程序型、流程型、元技能型；共同關鍵詞：api, asks, defaults, openai
- security-threat-model（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：architecture, asks, general, quick
- speech（openai-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：api, asks, defaults, openai

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
