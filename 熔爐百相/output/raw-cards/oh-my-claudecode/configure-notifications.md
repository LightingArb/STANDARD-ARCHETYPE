---
## oh-my-claudecode / configure-notifications

### 來源
- repo：oh-my-claudecode
- 路徑：skills/configure-notifications/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：oh-my-claudecode::skills/configure-notifications/skill.md

### 一句話定位
Configure notification integrations (Telegram, Discord, Slack) via natural lang…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- Use AskUserQuestion:
- Use AskUserQuestion with multiSelect:

禁止做
- Guide the user through creating a bot if they don't have one:
- CREATE A BOT (if you don't have one):
- Guide the user through creating a webhook if they don't have one:
- Users can skip this wizard entirely by setting env vars in their shell profile:

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
- [Hook Event Templates]
- Use AskUserQuestion:
- **Question:** "Which event would you like to configure templates for?"
- **Options:**
- 1. **session-end** - When a Claude session finishes (most common)
- 2. **ask-user-question** - When Claude is waiting for input
- 3. **session-idle** - When Claude finishes and waits for input

### 適用場景
- 適合在需要「Configure notification integrations (Telegram, Discord, Slack) via natural lang…」的工作階段使用。

### 原文精華摘錄
> Configure notification integrations (Telegram, Discord, Slack) via natural language
> Routing
> Detect which provider the user wants based on their request or argument:
> - If the trigger or argument contains "telegram" → follow the **Telegram** section
> - If the trigger or argument contains "discord" → follow the **Discord** section
> - If the trigger or argument contains "slack" → follow the **Slack** section
> Telegram Setup
> Guide the user through creating a bot if they don't have one:
> Use AskUserQuestion:
> **Question:** "Paste your Telegram bot token (from @BotFather)"
> **Validate** the token:
> Discord Setup
> **Question:** "How would you like to send Discord notifications?"
> **Options:**
> 1. **Webhook (Recommended)** - Create a webhook in your Discord channel. Simple, no bot needed. Just paste the URL.
> Slack Setup
> Guide the user through creating a webhook if they don't have one:
> **Question:** "Paste your Slack incoming webhook URL (starts with https://hooks.slack.com/services/...)"
> **Validate** the URL:
> Platform Activation Flags

### 和其他 skill 的潛在關聯
- analytics-tracking（marketingskills） - 相似 - 共享領域：meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：custom, event, integrations, related
- paid-ads（marketingskills） - 相似 - 共享領域：meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：integrations, platform, related, setup
- setup-deploy（gstack） - 互補 - 共享分類：工具程序型、流程型；共同關鍵詞：configure, custom, platform, routing
- codex（gstack） - 互補 - 共享領域：meta；共享分類：工具程序型、流程型；共同關鍵詞：cli, platform, routing, via

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
