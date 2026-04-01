---
## gstack / careful

### 來源
- repo：gstack
- 路徑：careful/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::careful/skill.md

### 一句話定位
為破壞性命令加上事前警告與人工覆核的安全護欄技能。

### 核心人格特質
務實, 操作導向, 證據導向, 懷疑論, 謹慎, 防呆導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- 原文過短，無法提取

禁止做
- 原文過短，無法提取

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「為破壞性命令加上事前警告與人工覆核的安全護欄技能」的工作階段使用。
- 常見觸發語句：be careful / safety mode / prod mode / careful mode

### 原文精華摘錄
> Safety guardrails for destructive commands
> Warns before rm -rf, DROP TABLE, force-push, git reset --hard, kubectl delete, and similar destructive operations
> User can override each warning
> Use when touching prod, debugging live systems, or working in a shared environment
> Use when asked to "be careful", "safety mode", "prod mode", or "careful mode"
> (gstack)
> What's protected
> | Pattern | Example | Risk |
> |---------|---------|------|
> | `rm -rf` / `rm -r` / `rm --recursive` | `rm -rf /var/data` | Recursive delete |

### 和其他 skill 的潛在關聯
- sora（openai-skills） - 相似 - 共享領域：safety；共享分類：工具程序型；共同關鍵詞：delete, environment, guardrails
- README（get-shit-done） - 相似 - 共享領域：safety；共享分類：工具程序型；共同關鍵詞：commands, works
- playwright（openai-skills） - 相似 - 共享領域：safety；共享分類：工具程序型；共同關鍵詞：guardrails
- security-ownership-map（openai-skills） - 相似 - 共享領域：safety；共享分類：工具程序型；共同關鍵詞：git

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [ ] 流程型
- [ ] 元技能型
---
