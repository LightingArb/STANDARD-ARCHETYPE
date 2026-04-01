---
## openai-skills / skill-creator

### 來源
- repo：openai-skills
- 路徑：skills/.system/skill-creator/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.system/skill-creator/skill.md

### 一句話定位
Guide for creating effective skills.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
- [Core Principles]
- **Default assumption: Codex is already very smart.** Only add context Codex doesn't already have. Challenge each piece of information: "Does Codex really need this explanation?" and "Does this paragraph justify its token cost?"
- Match the level of specificity to the task's fragility and variability:
- **High freedom (text-based instructions)**: Use when multiple approaches are valid, decisions depend on context, or heuristics guide the approach.
- **Medium freedom (pseudocode or scripts with parameters)**: Use when a preferred pattern exists, some variation is acceptable, or configuration affects behavior.
- **Low freedom (specific scripts, few parameters)**: Use when operations are fragile and error-prone, consistency is critical, or a specific sequence must be followed.
- Every skill consists of a required SKILL.md file and optional bundled resources:
- [Skill Creation Process]
- Skill creation involves these steps:
- 1. Understand the skill with concrete examples
- 2. Plan reusable skill contents (scripts, references, assets)
- 3. Initialize the skill (run init_skill.py)
- 4. Edit the skill (implement resources and write SKILL.md)
- 5. Validate the skill (run quick_validate.py)

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- Do not include any other fields in YAML frontmatter.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Guide for creating effective skills.」的工作階段使用。

### 原文精華摘錄
> Guide for creating effective skills
> This skill should be used when users want to create a new skill (or update an existing skill) that extends Codex's capabilities with specialized knowledge, workflows, or tool integrations
> About Skills
> 1. Specialized workflows - Multi-step procedures for specific domains
> 2. Tool integrations - Instructions for working with specific file formats or APIs
> 3. Domain expertise - Company-specific knowledge, schemas, business logic
> 4. Bundled resources - Scripts, references, and assets for complex and repetitive tasks
> Core Principles
> **Default assumption: Codex is already very smart.** Only add context Codex doesn't already have. Challenge each piece of information: "Does Codex really need this explanation?" and "Does this paragraph justify its token cost?"
> Match the level of specificity to the task's fragility and variability:

### 和其他 skill 的潛在關聯
- docx（anthropics-skills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：creating, documents, editing, existing
- email-sequence（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：core, integrations, principles, should
- referral-program（marketingskills） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：existing, integrations, new, tool
- ui-styling（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：core, creating, guide, quick

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
