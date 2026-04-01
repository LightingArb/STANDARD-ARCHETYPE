---
## oh-my-claudecode / README

### 來源
- repo：oh-my-claudecode
- 路徑：benchmark/README.md
- 檔案類型：README.md
- card_kind：overview
- language：en
- canonical_group：oh-my-claudecode::benchmark/readme.md

### 一句話定位
Automated benchmark comparison between vanilla Claude Code and OMC-enhanced Cla…

### 核心人格特質
結構化, 批判性, 風險敏感, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
- [Workflow]
- 1. **Setup** (one-time):
- 2. **Quick Test** (recommended):
- 3. **Full Benchmark**:
- 4. **Review Results**:
- - Check `results/comparison_report.md`
- - Inspect predictions in `predictions/vanilla/` and `predictions/omc/`

### 核心行為規則
必須做
- ./run_full_comparison.sh --skip-vanilla # Only run OMC (reuse vanilla results)

禁止做
- 2. **Parallel Runs**: Don't run vanilla and OMC in parallel (share API rate limits)

### 提問方式
無明確提問模板

### 審查維度
非審查型

### 輸出格式要求
無特定格式要求

### 適用場景
- 適合在需要「Automated benchmark comparison between vanilla Claude Code and OMC-enhanced Cla…」的工作階段使用。

### 原文精華摘錄
> Automated benchmark comparison between vanilla Claude Code and OMC-enhanced Claude Code
> Quick Start
> Scripts
> One-time setup and verification:
> - Installs Python dependencies
> - Builds Docker image for SWE-bench
> - Downloads and caches dataset
> Directory Structure
> Prerequisites
> - Docker

### 和其他 skill 的潛在關聯
- skill-creator（anthropics-skills） - 相似 - 共享領域：meta, performance, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：advanced, benchmark, comparison, performance
- winui-app（openai-skills） - 相似 - 共享領域：design, meta, performance；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：performance, troubleshooting
- react-best-practices（vercel-agent-skills） - 相似 - 共享領域：design, meta, performance；共享分類：審查型、工具程序型、元技能型；共同關鍵詞：performance, quick
- README（ui-ux-pro-max-skill） - 相似 - 共享領域：design, meta, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：advanced, license, prerequisites, usage

### 分類標記
- [ ] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
