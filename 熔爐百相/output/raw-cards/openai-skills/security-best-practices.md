---
## openai-skills / security-best-practices

### 來源
- repo：openai-skills
- 路徑：skills/.curated/security-best-practices/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/security-best-practices/skill.md

### 一句話定位
"Perform language and framework specific security best-practice reviews and sug…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Overview]
- This skill provides a description of how to identify the language and frameworks used by the current context, and then to load information from this skill's references directory about the security best practices for this language and or frameworks.
- This information, if present, can be used to write new secure by default code, or to passively detect major issues within existing code, or (if requested by the user) provide a vulnerability report and suggest fixes.
- [Workflow]
- 1. The primary mode is to just use the information to write secure by default code from this point forward. This is useful for starting a new project or when writing new code.
- 2. The secondary mode is to passively detect vulnerabilities while working in the project and writing code for the user. Critical or very important vulnerabilities or major issues going against security guidance can be flagged and the user can be told about them. This passive mode should focus on the largest impact vulnerabilities and secure defaults.
- 3. The user can ask for a security report or to improve the security of the codebase. In this case a full report should be produced describe anyways the project fails to follow security best practices guidance. The report should be prioritized and have clear sections of severity and urgency. Then offer to start working on fixes for these issues. See #fixes below.
- [Workflow Decision Tree]
- - If the language/framework is unclear, inspect the repo to determine it and list your evidence.
- - If matching guidance exists in `references/`, load only the relevant files and follow their instructions.
- - If no matching guidance exists, consider if you know any well known security best practices for the chosen language and or frameworks, but if asked to generate a report, let the user know that concrete guidance is not available (you can still generate the report or detect for sure critical vulnerabilities)

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
- "Perform language and framework specific security best-practice reviews and suggest improvements
- Trigger only when the user explicitly requests security best practices guidance, a security review/report, or secure-by-default coding help
- Do not trigger for general code review, debugging, or non-security tasks."

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「"Perform language and framework specific security best-practice reviews and sug…」的工作階段使用。
- 常見觸發語句：Perform language and framework specific security best-practice reviews and suggest improvements. Trigger only when the user explicitly requests security best practices guidance, a security review/report, or secure-by-default coding help. Trigger only for supported languages (python, javascript/typescript, go). Do not trigger for general code review, debugging, or non-security tasks.
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> "Perform language and framework specific security best-practice reviews and suggest improvements
> Trigger only when the user explicitly requests security best practices guidance, a security review/report, or secure-by-default coding help
> Trigger only for supported languages (python, javascript/typescript, go)
> Do not trigger for general code review, debugging, or non-security tasks."
> Overview
> This skill provides a description of how to identify the language and frameworks used by the current context, and then to load information from this skill's references directory about the security best practices for this language and or frameworks.
> This information, if present, can be used to write new secure by default code, or to passively detect major issues within existing code, or (if requested by the user) provide a vulnerability report and suggest fixes.
> Workflow
> 1. The primary mode is to just use the information to write secure by default code from this point forward. This is useful for starting a new project or when writing new code.
> 2. The secondary mode is to passively detect vulnerabilities while working in the project and writing code for the user. Critical or very important vulnerabilities or major issues going against security guidance can be flagged and the user can be told about...

### 和其他 skill 的潛在關聯
- webapp-testing（anthropics-skills） - 相似 - 共享領域：debugging, design, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：best, decision, practices, tree
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：best, overview, practices, python
- doc-coauthoring（anthropics-skills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：decision, guidance, tasks, trigger
- design-html（gstack） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：framework, report, suggest

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
