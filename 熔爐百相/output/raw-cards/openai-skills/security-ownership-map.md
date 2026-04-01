---
## openai-skills / security-ownership-map

### 來源
- repo：openai-skills
- 路徑：skills/.curated/security-ownership-map/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：openai-skills::skills/.curated/security-ownership-map/skill.md

### 一句話定位
"Analyze git repositories to build a security ownership topology (people-to-fil…

### 核心人格特質
框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向, 規範導向, 元認知, 品味導向

### 核心思考框架
- [Overview]
- Build a bipartite graph of people and files from git history, then compute ownership risk and export graph artifacts for Neo4j/Gephi. Also build a file co-change graph (Jaccard similarity on shared commits) to cluster files by how they move together while ignoring large, noisy commits.
- [Workflow]
- 1. Scope the repo and time window (optional `--since/--until`).
- 2. Decide sensitivity rules (use defaults or provide a CSV config).
- 3. Build the ownership map with `scripts/run_ownership_map.py` (co-change graph is on by default; use `--cochange-max-files` to ignore supernode commits).
- 4. Communities are computed by default; graphml output is optional (`--graphml`).
- 5. Query the outputs with `scripts/query_ownership.py` for bounded JSON slices.
- 6. Persist and visualize (see `references/neo4j-import.md`).

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
- [Output artifacts]
- `ownership-map-out/` contains:
- - `people.csv` (nodes: people)
- - `files.csv` (nodes: files)
- - `edges.csv` (edges: touches)
- - `cochange_edges.csv` (file-to-file co-change edges with Jaccard weight; omitted with `--no-cochange`)
- - `summary.json` (security ownership findings)

### 適用場景
- 適合在需要「"Analyze git repositories to build a security ownership topology (people-to-fil…」的工作階段使用。
- 常見觸發語句：Analyze git repositories to build a security ownership topology (people-to-file), compute bus factor and sensitive-code ownership, and export CSV/JSON for graph databases and visualization. Trigger only when the user explicitly wants a security-oriented ownership or bus-factor analysis grounded in git history (for example: orphaned sensitive code, security maintainers, CODEOWNERS reality checks for risk, sensitive hotspots, or ownership clusters). Do not trigger for general maintainer lists or non-security ownership questions.
- 常出現在收尾、發布、分支整理或部署交接階段。

### 原文精華摘錄
> "Analyze git repositories to build a security ownership topology (people-to-file), compute bus factor and sensitive-code ownership, and export CSV/JSON for graph databases and visualization
> Trigger only when the user explicitly wants a security-oriented ownership or bus-factor analysis grounded in git history (for example: orphaned sensitive code, security maintainers, CODEOWNERS reality checks for risk,...
> Do not trigger for general maintainer lists or non-security ownership questions."
> Overview
> Build a bipartite graph of people and files from git history, then compute ownership risk and export graph artifacts for Neo4j/Gephi. Also build a file co-change graph (Jaccard similarity on shared commits) to cluster files by how they move together while i...
> Requirements
> - Python 3
> - `networkx` (required; community detection is enabled by default)
> Install with:
> Workflow

### 和其他 skill 的潛在關聯
- README（oh-my-claudecode） - 相似 - 共享領域：design, learning, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：history, quick, requirements, start
- monetization-strategy（pm-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：example, notes, output, requirements
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, planning；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：csv, output, overview, requirements
- banner-design（ui-ux-pro-max-skill） - 相似 - 共享領域：design, learning, meta；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：quick, rules, security

### 分類標記
- [x] 思考框架型
- [ ] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
