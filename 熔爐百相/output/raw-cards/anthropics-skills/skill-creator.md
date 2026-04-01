---
## anthropics-skills / skill-creator

### 來源
- repo：anthropics-skills
- 路徑：skills/skill-creator/SKILL.md
- 檔案類型：SKILL.md
- card_kind：skill
- language：en
- canonical_group：anthropics-skills::skills/skill-creator/skill.md

### 一句話定位
Create new skills, modify and improve existing skills, and measure skill perfor…

### 核心人格特質
結構化, 批判性, 風險敏感, 框架導向, 第一性原理, 推理導向, 流程紀律, 完成導向, 務實, 操作導向

### 核心思考框架
原文過短，無法提取

### 核心行為規則
必須做
- ALWAYS use this exact template:
- 3. **Explain the why.** Try hard to explain the **why** behind everything you're asking the model to do. Today's LLMs are *smart*. They have good theory of mind and when given a good harness can go beyond rote instructions and really make things happen. Even if the feedback from the user is terse or frustrated, try to actually understand the task and why the user is writing what they wrote, and what they actually wrote, and then transmit this understanding into the instructions. If you find yourself writing ALWAYS or NEVER in all caps, or using super rigid structures, that's a yellow flag — if possible, reframe and explain the reasoning so that the model understands why the thing you're asking for is important. That's a more humane, powerful, and effective approach.
- Try to explain to the model why things are important in lieu of heavy-handed musty MUSTs. Use theory of mind and try to make the skill general and not super-narrow to specific examples. Start by writing a draft and then look at it with fresh eyes and improve it.
- - **Improving an existing skill**: the old version. Before editing, snapshot the skill (`cp -r <skill-path> <workspace>/skill-snapshot/`), then point the baseline subagent at the snapshot. Save to `old_skill/outputs/`.
- 1. **Generalize from the feedback.** The big picture thing that's happening here is that we're trying to create skills that can be used a million times (maybe literally, maybe even more who knows) across many different prompts. Here you and the user are iterating on only a few examples over and over again because it helps move faster. The user knows these examples in and out and it's quick for them to assess new outputs. But if the skill you and the user are codeveloping works only for those examples, it's useless. Rather than put in fiddly overfitty changes, or oppressively constrictive MUSTs, if there's some stubborn issue, you might try branching out and using different metaphors, or recommending different patterns of working. It's relatively cheap to try and maybe you'll land on something great.

禁止做
- Of course, you should always be flexible and if the user is like "I don't need to run a bunch of evaluations, just vibe with me", you can do that instead.
- 4. Should we set up test cases to verify the skill works? Skills with objectively verifiable outputs (file transforms, data extraction, code generation, fixed workflow steps) benefit from test cases. Skills with subjective outputs (writing style, art) often don't need them. Suggest the appropriate default based on the skill type, but let the user decide.
- - **description**: When to trigger, what it does. This is the primary triggering mechanism - include both what the skill does AND specific contexts for when to use it. All "when to use" info goes here, not in the body. Note: currently Claude has a tendency to "undertrigger" skills -- to not use them when they'd be useful. To combat this, please make the skill descriptions a little bit "pushy". So for instance, instead of "How to build a simple fast dashboard to display internal Anthropic data.", you might write "How to build a simple fast dashboard to display internal Anthropic data. Make sure to use this skill whenever the user mentions dashboards, data visualization, internal metrics, or wants to display any kind of company data, even if they don't explicitly ask for a 'dashboard.'"
- This goes without saying, but skills must not contain malware, exploit code, or any content that could compromise system security. A skill's contents should not surprise the user in their intent if described. Don't go along with requests to create misleading skills or skills designed to facilitate unauthorized access, data exfiltration, or other malicious activities. Things like a "roleplay as an XYZ" are OK though.
- After writing the skill draft, come up with 2-3 realistic test prompts — the kind of thing a real user would actually say. Share them with the user: [you don't have to use this exact language] "Here are a few test cases I'd like to try. Do these look right, or do you want to add more?" Then run them.
- Save test cases to `evals/evals.json`. Don't write assertions yet — just the prompts. You'll draft assertions in the next step while the runs are in progress.
- This section is one continuous sequence — don't stop partway through. Do NOT use `/skill-test` or any other testing skill.
- Put results in `<skill-name>-workspace/` as a sibling to the skill directory. Within the workspace, organize results by iteration (`iteration-1/`, `iteration-2/`, etc.) and within that, each test case gets a directory (`eval-0/`, `eval-1/`, etc.). Don't create all of this upfront — just create directories as you go.
- For each test case, spawn two subagents in the same turn — one with the skill, one without. This is important: don't spawn the with-skill runs first and then come back for baselines later. Launch everything at once so it all finishes around the same time.
- Write an `eval_metadata.json` for each test case (assertions can be empty for now). Give each eval a descriptive name based on what it's testing — not just "eval-0". Use this name for the directory too. If this iteration uses new or modified eval prompts, create these files for each new eval directory — don't assume they carry over from previous iterations.
- Don't just wait for the runs to finish — you can use this time productively. Draft quantitative assertions for each test case and explain them to the user. If assertions already exist in `evals/evals.json`, review them and explain what they check.
- Good assertions are objectively verifiable and have descriptive names — they should read clearly in the benchmark viewer so someone glancing at the results immediately understands what each one checks. Subjective skills (writing style, design quality) are better evaluated qualitatively — don't force assertions onto things that need human judgment.

### 提問方式
- 1. What should this skill enable Claude to do?
- 3. What's the expected output format?

### 審查維度
- Use when users want to create a skill from scratch, edit, or optimize an existing skill, run evals to test a skill, benchmark skill performance with variance analysis, or optimize a skill's description for better trig...

### 輸出格式要求
- [Report structure]
- ALWAYS use this exact template:
- [Executive summary]
- [Commit message format]
- **Example 1:**

### 適用場景
- 適合在需要「Create new skills, modify and improve existing skills, and measure skill perfor…」的工作階段使用。

### 原文精華摘錄
> Create new skills, modify and improve existing skills, and measure skill performance
> Use when users want to create a skill from scratch, edit, or optimize an existing skill, run evals to test a skill, benchmark skill performance with variance analysis, or optimize a skill's description for better trig...
> Communicating with the user
> So please pay attention to context cues to understand how to phrase your communication! In the default case, just to give you some idea:
> - "evaluation" and "benchmark" are borderline, but OK
> - for "JSON" and "assertion" you want to see serious cues from the user that they know what those things are before using them without explaining them
> Creating a skill
> 1. What should this skill enable Claude to do?
> 2. When should this skill trigger? (what user phrases/contexts)
> 3. What's the expected output format?

### 和其他 skill 的潛在關聯
- ad-creative（marketingskills） - 相似 - 共享領域：meta, performance, shipping；共享分類：思考框架型、工具程序型、流程型、元技能型；共同關鍵詞：existing, new, optimization, performance
- ship（gstack） - 相似 - 共享領域：shipping, testing；共享分類：審查型、工具程序型、流程型；共同關鍵詞：cases, commit, format, key
- ab-test-setup（marketingskills） - 相似 - 共享領域：meta, shipping, testing；共享分類：思考框架型、審查型、工具程序型、流程型、元技能型；共同關鍵詞：better, measure, optimization, running
- README（oh-my-claudecode） - 相似 - 共享領域：meta, performance, shipping；共享分類：審查型、工具程序型、流程型、元技能型；共同關鍵詞：advanced, benchmark, comparison, performance

### 分類標記
- [x] 思考框架型
- [x] 審查型
- [x] 工具程序型
- [ ] 人格型
- [x] 流程型
- [x] 元技能型
---
