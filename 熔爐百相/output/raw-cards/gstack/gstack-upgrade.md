---
## gstack / gstack-upgrade

### 來源
- repo：gstack
- 路徑：gstack-upgrade/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：gstack::gstack-upgrade/skill.md

### 一句話定位
將 gstack 升級到最新版本並處理升級提示的維護技能。

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 證據導向, 懷疑論

### 核心思考框架
- [Inline upgrade flow]
- First, check if auto-upgrade is enabled:
- **If `AUTO_UPGRADE=true` or `AUTO_UPGRADE=1`:** Skip AskUserQuestion. Log "Auto-upgrading gstack v{old} → v{new}..." and proceed directly to Step 2. If `./setup` fails during auto-upgrade, restore from backup (`.bak` directory) and warn the user: "Auto-upgrade failed — restored previous version. Run `/gstack-upgrade` manually to retry."
- **Otherwise**, use AskUserQuestion:
- - Question: "gstack **v{new}** is available (you're on v{old}). Upgrade now?"
- - Options: ["Yes, upgrade now", "Always keep me up to date", "Not now", "Never ask again"]
- **If "Yes, upgrade now":** Proceed to Step 2.

### 核心行為規則
必須做
- ### Step 1: Ask the user (or auto-upgrade)

禁止做
- - Options: ["Yes, upgrade now", "Always keep me up to date", "Not now", "Never ask again"]
- **If "Not now":** Write snooze state with escalating backoff (first snooze = 24h, second = 48h, third+ = 1 week), then continue with the current skill. Do not mention the upgrade again.
- **If "Never ask again":**
- Read `$INSTALL_DIR/CHANGELOG.md`. Find all version entries between the old version and the new version. Summarize as 5-7 bullets grouped by theme. Don't overwhelm — focus on user-facing changes. Skip internal refactors unless they're significant.

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「將 gstack 升級到最新版本並處理升級提示的維護技能」的工作階段使用。
- 常見觸發語句：upgrade gstack / update gstack / get latest version

### 原文精華摘錄
> Upgrade gstack to the latest version
> Detects global vs vendored install, runs the upgrade, and shows what's new
> Use when asked to "upgrade gstack", "update gstack", or "get latest version"
> Inline upgrade flow
> First, check if auto-upgrade is enabled:
> **If `AUTO_UPGRADE=true` or `AUTO_UPGRADE=1`:** Skip AskUserQuestion. Log "Auto-upgrading gstack v{old} → v{new}..." and proceed directly to Step 2. If `./setup` fails during auto-upgrade, restore from backup (`.bak` directory) and warn the user: "Auto-upgr...
> **Otherwise**, use AskUserQuestion:
> - Question: "gstack **v{new}** is available (you're on v{old}). Upgrade now?"
> Standalone usage
> When invoked directly as `/gstack-upgrade` (not from a preamble):

### 和其他 skill 的潛在關聯
- README（ui-ux-pro-max-skill） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型；共同關鍵詞：new, usage, what
- theme-factory（anthropics-skills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型；共同關鍵詞：new, usage
- xlsx（anthropics-skills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型；共同關鍵詞：new, standalone
- churn-prevention（marketingskills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型；共同關鍵詞：flow, upgrade

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
