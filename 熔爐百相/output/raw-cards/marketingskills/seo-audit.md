---
## marketingskills / seo-audit

### 來源
- repo：marketingskills
- 路徑：skills/seo-audit/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：marketingskills::skills/seo-audit/skill.md

### 一句話定位
When the user wants to audit, review, or diagnose SEO issues on their site.

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 務實, 操作導向, 規範導向, 元認知

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- Before auditing, understand:

禁止做
- **`web_fetch` and `curl` cannot reliably detect structured data / schema markup.**
- > **Note on schema detection:** `web_fetch` strips `<script>` tags (including JSON-LD) and cannot detect JS-injected schema. Use the browser tool, Rich Results Test, or Screaming Frog instead — they render JavaScript and capture dynamically-injected markup. See the Schema Markup Detection Limitation section above.

### 提問方式
- - What's the primary business goal for SEO?
- - What keywords/topics are priorities?
- - Any known issues or concerns?
- - Current organic traffic level?
- - Recent changes or migrations?
- - Full site audit or specific pages?
- - Technical + on-page, or one focus area?
- - Access to Search Console / analytics?
- [Task-Specific Questions]
- 1. What pages/keywords matter most?
- 2. Do you have Search Console access?
- 3. Any recent changes or migrations?

### 審查維度
- [Audit Framework]
- **`web_fetch` and `curl` cannot reliably detect structured data / schema markup.**
- **To accurately check for schema markup, use one of these methods:**
- 1. **Browser tool** — render the page and run: `document.querySelectorAll('script[type="application/ld+json"]')`
- 2. **Google Rich Results Test** — https://search.google.com/test/rich-results
- 3. **Screaming Frog export** — if the client provides one, use it (SF renders JavaScript)
- 1. **Crawlability & Indexation** (can Google find and index it?)
- [Technical SEO Audit]
- **Robots.txt**
- - Check for unintentional blocks
- - Verify important pages allowed
- - Check sitemap reference
- **XML Sitemap**
- - Exists and accessible
- [On-Page SEO Audit]
- **Check for:**

### 輸出格式要求
- [Output Format]
- **Executive Summary**
- - Overall health assessment
- - Top 3-5 priority issues
- - Quick wins identified
- **Technical SEO Findings**
- For each issue:

### 適用場景
- 適合在需要「When the user wants to audit, review, or diagnose SEO issues on their site.」的工作階段使用。
- 常見觸發語句：SEO audit, / technical SEO, / why am I not ranking, / SEO issues, / on-page SEO, / meta tags review, / SEO health check, / my traffic dropped,
- 常出現在審查、驗證、合併前檢查或上線後回看階段。

### 原文精華摘錄
> When the user wants to audit, review, or diagnose SEO issues on their site
> Also use when the user mentions "SEO audit," "technical SEO," "why am I not ranking," "SEO issues," "on-page SEO," "meta tags review," "SEO health check," "my traffic dropped," "lost rankings," "not showing up in Goog...
> For building pages at scale to target keywords, see programmatic-seo
> For adding structured data, see schema-markup
> For AI search optimization, see ai-seo
> Initial Assessment
> **Check for product marketing context first:**
> Before auditing, understand:
> 1. **Site Context**
> - What type of site? (SaaS, e-commerce, blog, etc.)

### 和其他 skill 的潛在關聯
- xlsx（anthropics-skills） - 相似 - 共享領域：design, meta, review；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：adding, also, common, data
- react-best-practices（vercel-agent-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、工具程序型、元技能型；共同關鍵詞：data, optimization, pages
- aspnet-core（openai-skills） - 相似 - 共享領域：browser, design, meta；共享分類：思考框架型、審查型、元技能型；共同關鍵詞：core, pages, web
- design-review（gstack） - 相似 - 共享領域：design, review；共享分類：審查型；共同關鍵詞：audit, building, format, framework

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [ ] 流程型
- [x] 元技能型
---
