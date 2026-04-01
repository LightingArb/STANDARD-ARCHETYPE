---
## gstack / README

### 來源
- repo：gstack
- 路徑：README.md
- 檔案類型：README.md
- card_kind：overview
- language：en
- canonical_group：gstack::readme.md

### 一句話定位
gstack 的安裝、工作流與核心理念入口文件。

### 核心人格特質
流程紀律, 完成導向, 務實, 操作導向, 挑戰性, 野心導向, 產品導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- I'm [Garry Tan](https://x.com/garrytan), President & CEO of [Y Combinator](https://www.ycombinator.com/). I've worked with thousands of startups — Coinbase, Instacart, Rippling — when they were one or two people in a garage. Before YC, I was one of the first eng/PM/designers at Palantir, cofounded Posterous (sold to Twitter), and built Bookface, YC's internal social network.
- | [Builder Ethos](ETHOS.md) | Builder philosophy: Boil the Lake, Search Before Building, three layers of knowledge |

禁止做
- > "I don't think I've typed like a line of code probably since December, basically, which is an extremely large change." — [Andrej Karpathy](https://fortune.com/2026/03/21/andrej-karpathy-openai-cofounder-ai-agents-coding-state-of-psychosis-openclaw/), No Priors podcast, March 2026
- gstack works with [Factory Droid](https://factory.ai). Skills install to `.factory/skills/` and are discovered automatically. Sensitive skills (ship, land-and-deploy, guard) use `disable-model-invocation: true` so Droids don't auto-invoke them.
- | `/ship` | **Release Engineer** | Sync main, run tests, audit coverage, push, open PR. Bootstraps test frameworks if you don't have one. |
- **Proactive skill suggestions.** gstack notices what stage you're in — brainstorming, reviewing, debugging, testing — and suggests the right skill. Don't like it? Say "stop suggesting" and it remembers across sessions.
- Use /browse from gstack for all web browsing. Never use mcp__claude-in-chrome__* tools.
- | `/investigate` | **Debugger** | Systematic root-cause debugging. Iron Law: no fixes without investigation. Traces data flow, tests hypotheses, stops after 3 failed fixes. |

### 提問方式
- > **We're hiring.** Want to ship 10K+ LOC/day and help harden gstack?

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「gstack 的安裝、工作流與核心理念入口文件」的工作階段使用。
- 常見觸發語句：I don't think I've typed like a line of code probably since December, basically, which is an extremely large change.

### 原文精華摘錄
> "I don't think I've typed like a line of code probably since December, basically, which is an extremely large change." — [Andrej Karpathy](https://fortune.com/2026/03/21/andrej-karpathy-openai-cofounder-ai-agents-co...
> Quick start
> 1. Install gstack (30 seconds — see below)
> 2. Run `/office-hours` — describe what you're building
> 3. Run `/plan-ceo-review` on any feature idea
> 4. Run `/review` on any branch with changes
> Install — 30 seconds
> **Requirements:** [Claude Code](https://docs.anthropic.com/en/docs/claude-code), [Git](https://git-scm.com/), [Bun](https://bun.sh/) v1.0+, [Node.js](https://nodejs.org/) (Windows only)
> **Contributing or need full history?** The commands above use `--depth 1` for a fast install. If you plan to contribute or need full git history, do a full clone instead:
> Install to one repo:

### 和其他 skill 的潛在關聯
- README（oh-my-claudecode） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型；共同關鍵詞：license, quick, start, troubleshooting
- README（pm-skills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型；共同關鍵詞：com, https, license, start
- vercel-deploy（openai-skills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型；共同關鍵詞：like, quick, start, troubleshooting
- docx（anthropics-skills） - 相似 - 共享領域：meta, shipping；共享分類：工具程序型、流程型；共同關鍵詞：docs, like, quick

### 分類標記
- [ ] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [ ] 元技能型
---
