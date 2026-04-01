---
## openai-skills / aspnet-core

### 來源
- repo：openai-skills
- 路徑：skills/.curated/aspnet-core/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/aspnet-core/skill.md

### 一句話定位
Build, review, refactor, or architect ASP.NET Core web applications using curre…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 規範導向, 元認知

### 核心思考框架
- [Overview]
- Choose the right ASP.NET Core application model, compose the host and request pipeline correctly, and implement features in the framework style Microsoft documents today.
- Load the smallest set of references that fits the task. Do not load every reference by default.
- [Workflow]
- 1. Confirm the target framework, SDK, and current app model.
- 2. Open [references/stack-selection.md](references/stack-selection.md) first for new apps or major refactors.
- 3. Open [references/program-and-pipeline.md](references/program-and-pipeline.md) next for `Program.cs`, DI, configuration, middleware, routing, logging, and static assets.
- 4. Open exactly one primary app-model reference:
- - [references/ui-blazor.md](references/ui-blazor.md)
- - [references/ui-razor-pages.md](references/ui-razor-pages.md)
- [Reference Guide]
- - [references/_sections.md](references/_sections.md): Quick index and reading order.
- - [references/stack-selection.md](references/stack-selection.md): Choose the right ASP.NET Core application model and template.
- - [references/program-and-pipeline.md](references/program-and-pipeline.md): Structure `Program.cs`, services, middleware, routing, configuration, logging, and static assets.
- - [references/ui-blazor.md](references/ui-blazor.md): Build Blazor Web Apps, choose render modes, and use components, forms, and JS interop correctly.
- - [references/ui-razor-pages.md](references/ui-razor-pages.md): Build page-focused server-rendered apps with handlers, model binding, and conventions.
- - [references/ui-mvc.md](references/ui-mvc.md): Build controller/view applications with clear separation of concerns.

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- Load the smallest set of references that fits the task. Do not load every reference by default.
- - Respect the existing app model. Do not rewrite Razor Pages to MVC or controllers to Minimal APIs without a clear reason.

### 提問方式
無明確提問模板

### 審查維度
- Build, review, refactor, or architect ASP.NET Core web applications using current official guidance for .NET web development

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Build, review, refactor, or architect ASP.NET Core web applications using curre…」的工作階段使用。
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> Build, review, refactor, or architect ASP.NET Core web applications using current official guidance for .NET web development
> Use when working on Blazor Web Apps, Razor Pages, MVC, Minimal APIs, controller-based Web APIs, SignalR, gRPC, middleware, dependency injection, configuration, authentication, authorization, testing, performance, depl...
> Overview
> Choose the right ASP.NET Core application model, compose the host and request pipeline correctly, and implement features in the framework style Microsoft documents today.
> Load the smallest set of references that fits the task. Do not load every reference by default.
> Workflow
> 1. Confirm the target framework, SDK, and current app model.
> 2. Open [references/stack-selection.md](references/stack-selection.md) first for new apps or major refactors.
> 3. Open [references/program-and-pipeline.md](references/program-and-pipeline.md) next for `Program.cs`, DI, configuration, middleware, routing, logging, and static assets.
> 4. Open exactly one primary app-model reference:

### 和其他 skill 的潛在關聯
- schema-markup（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、流程型、元技能型；共同關鍵詞：core, pages, reference, testing
- webapp-testing（anthropics-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、流程型、元技能型；共同關鍵詞：applications, reference, testing, web
- seo-audit（marketingskills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、元技能型；共同關鍵詞：core, pages, web
- react-best-practices（vercel-agent-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、元技能型；共同關鍵詞：pages, performance, reference

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [ ] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
