---
## openai-skills / winui-app

### 來源
- repo：openai-skills
- 路徑：skills/.curated/winui-app/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/winui-app/skill.md

### 一句話定位
Bootstrap, develop, and design modern WinUI 3 desktop applications with C and t…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Required Flow]
- 1. Classify the task as environment/setup, new-app bootstrap, design, implementation, review, or troubleshooting.
- 2. If the task is about preparing a machine for WinUI, auditing readiness, or creating a brand new app, start with the bundled setup-and-scaffold flow in this skill before broader design, implementation, or troubleshooting work:
- - Pick the app name when the request is for a new app.
- - Use the exact name the user gave when it is already a safe folder name.
- - If the user did not give a name, derive a short PascalCase name from the request and state what you chose.
- - Create the project in the user's current workspace unless they asked for another location.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- - Do not use `--force` unless the user explicitly asked to overwrite existing files.
- - For a brand new app, scaffold with `dotnet new winui -o <name>`. Add template options only when the user asked for them. Supported options: `-f|--framework net10.0|net9.0|net8.0`, `-slnx|--use-slnx`, `-cpm|--central-pkg-mgmt`, `-mvvm|--use-mvvm`, `-imt|--include-mvvm-toolkit`, `-un|--unpackaged`, `-nsf|--no-solution-file`, `--force`. Do not invent unsupported flags. If the user asks for packaged behavior, pass `--unpackaged false`. Otherwise keep the template default.
- 13. Do not invent app-specific controls, bespoke component libraries, or custom chrome to replace stock WinUI behavior unless the user explicitly asks for that customization, the existing product design system already requires it, or a verified platform gap leaves no clean native option.
- 19. Do not add extra `Border` wrappers around sections, lists, or cards unless the border is doing distinct work that the contained control or parent surface does not already provide. Avoid "double-card" compositions where a section `Border` wraps child items that already render as cards.
- - Do not guess whether the machine is ready for WinUI development. Verify it.
- - Treat WinUI design guidance and native controls as the baseline. Do not drift into bespoke component systems or app-specific replacements for standard controls unless the user explicitly requests them or the existing codebase already depends on them.

### 提問方式
無明確提問模板

### 審查維度
- Use when creating a brand new app, preparing a machine for WinUI, reviewing, refactoring, planning, troubleshooting, environment-checking, or setting up WinUI 3 XAML, controls, navigation, windowing, theming, accessib...

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Bootstrap, develop, and design modern WinUI 3 desktop applications with C and t…」的工作階段使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Bootstrap, develop, and design modern WinUI 3 desktop applications with C# and the Windows App SDK using official Microsoft guidance, WinUI Gallery patterns, Windows App SDK samples, and CommunityToolkit components
> Use when creating a brand new app, preparing a machine for WinUI, reviewing, refactoring, planning, troubleshooting, environment-checking, or setting up WinUI 3 XAML, controls, navigation, windowing, theming, accessib...
> Required Flow
> 1. Classify the task as environment/setup, new-app bootstrap, design, implementation, review, or troubleshooting.
> 2. If the task is about preparing a machine for WinUI, auditing readiness, or creating a brand new app, start with the bundled setup-and-scaffold flow in this skill before broader design, implementation, or troubleshooting work:
> - Pick the app name when the request is for a new app.
> - Use the exact name the user gave when it is already a safe folder name.
> Common Routes
> | Request | Read first |
> | --- | --- |

### 和其他 skill 的潛在關聯
- ui-ux-pro-max（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：accessibility, app, common, reference
- react-best-practices（vercel-agent-skills） - 相似 - 共享領域：design, meta, performance；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：patterns, performance, refactoring, reference
- pptx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：審查型、流程型、元技能型；共同關鍵詞：creating, reference, required
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：common, new

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
