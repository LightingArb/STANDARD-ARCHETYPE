from __future__ import annotations

# === REGISTERED_REPOS ===
# 本標記由分解熔爐 Step 0 比對用，手動維護。
# 格式：每行一個 repo name，與 config.json 的 name 欄位一致。
# 最後更新：2026-03-31
REGISTERED_REPOS = [
    "gstack",
    "superpowers",
    "oh-my-claudecode",
    "get-shit-done",
    "ui-ux-pro-max-skill",
    "pm-skills",
    "marketingskills",
    "anthropics-skills",
    "openai-skills",
    "vercel-agent-skills",
    "awesome-startup",
    "web-access",
    "gcloud-mcp",
    "chrome-devtools-mcp",
]
# === END REGISTERED_REPOS ===

import hashlib
import json
import re
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).parent
CONFIG = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
REPOS_ROOT = ROOT / "repos"
OUTPUT_ROOT = ROOT / "output"
RAW_CARDS_ROOT = OUTPUT_ROOT / "raw-cards"

TARGET_FILE_NAMES = {"SKILL.md", "CLAUDE.md", "AGENTS.md"}
ROOT_README = "README.md"
MAIN_CARD_KINDS = {"skill", "agent", "knowledge"}
ALL_CARD_KINDS = ["skill", "agent", "knowledge", "overview", "template", "translation", "tool-reference"]

EXCLUDED_PARTS = {
    ".git",
    ".github",
    "node_modules",
    "vendor",
    "dist",
    "build",
    ".next",
    ".venv",
    "venv",
    "__pycache__",
}

LANG_MAP = {
    "/ja-jp/": "ja",
    "/ja/": "ja",
    "/ko-kr/": "ko",
    "/ko/": "ko",
    "/pt-br/": "pt-BR",
    "/pt/": "pt-BR",
    "/zh-cn/": "zh-CN",
    "/zh/": "zh-CN",
    "/zh-tw/": "zh-TW",
    "/es/": "es",
    "/fr/": "fr",
    "/de/": "de",
}

COMMON_STOPWORDS = {
    "a",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "before",
    "by",
    "can",
    "check",
    "code",
    "create",
    "default",
    "do",
    "each",
    "for",
    "from",
    "get",
    "how",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "mode",
    "not",
    "of",
    "on",
    "or",
    "out",
    "run",
    "skill",
    "skills",
    "that",
    "the",
    "their",
    "them",
    "then",
    "this",
    "to",
    "use",
    "user",
    "using",
    "when",
    "with",
    "you",
    "your",
}

SIMILARITY_STOPWORDS = COMMON_STOPWORDS | {
    "agents",
    "claude",
    "complete",
    "components",
    "debugging",
    "design",
    "development",
    "implementation",
    "meta",
    "parallel",
    "planning",
    "readme",
    "review",
    "session",
    "shipping",
    "superpowers",
    "workflow",
}

FRAMEWORK_HEADING_KEYWORDS = (
    "process",
    "phase",
    "phases",
    "philosophy",
    "principle",
    "principles",
    "pattern",
    "patterns",
    "workflow",
    "workflows",
    "checklist",
    "hierarchy",
    "decision",
    "decisions",
    "modes",
    "flow",
    "guide",
    "overview",
    "iron law",
    "review sections",
    "implementation order",
    "mapping",
)

QUESTION_HEADING_KEYWORDS = (
    "askuserquestion",
    "question",
    "questions",
    "follow-up",
    "forcing questions",
)

REVIEW_HEADING_KEYWORDS = (
    "review",
    "audit",
    "rubric",
    "checklist",
    "items",
    "sections",
    "dimension",
    "dimensions",
    "score",
    "coverage",
    "health",
)

FORMAT_HEADING_KEYWORDS = (
    "required outputs",
    "output",
    "format",
    "formatting",
    "report",
    "dashboard",
    "footer",
    "template",
    "summary",
)

DOMAIN_KEYWORDS = {
    "design": [
        "design",
        "designer",
        "ui",
        "ux",
        "visual",
        "typography",
        "layout",
        "motion",
        "aesthetic",
    ],
    "review": [
        "review",
        "audit",
        "qa",
        "checklist",
        "rubric",
        "score",
        "verify",
        "verification",
    ],
    "debugging": [
        "debug",
        "bug",
        "failure",
        "root cause",
        "unexpected behavior",
        "hypothesis",
        "trace",
    ],
    "shipping": [
        "ship",
        "deploy",
        "release",
        "merge",
        "branch",
        "pr",
        "pull request",
        "changelog",
    ],
    "planning": [
        "plan",
        "scope",
        "requirements",
        "brainstorm",
        "architecture",
        "founder",
        "ceo",
    ],
    "safety": [
        "safety",
        "careful",
        "freeze",
        "guard",
        "destructive",
        "security",
        "risk",
    ],
    "browser": [
        "browse",
        "browser",
        "chrome",
        "cookie",
        "screenshot",
        "page",
        "dialog",
        "headless",
    ],
    "meta": [
        "skill",
        "skills",
        "agents",
        "claude",
        "superpowers",
    ],
    "parallel": [
        "parallel",
        "subagent",
        "worktree",
        "independent tasks",
    ],
    "learning": [
        "learn",
        "learnings",
        "retro",
        "history",
        "trend",
        "telemetry",
    ],
    "performance": [
        "performance",
        "benchmark",
        "core web vitals",
        "regression",
        "page load",
        "canary",
    ],
    "testing": [
        "test",
        "tests",
        "coverage",
        "tdd",
        "red-green-refactor",
        "passing",
    ],
}

ONE_LINERS = {
    ("gstack", "AGENTS.md"): "gstack 倉庫內的技能總覽、建置命令與協作慣例說明。",
    ("gstack", "CLAUDE.md"): "gstack 倉庫的工程規範、文件生成與提交流程總指引。",
    ("gstack", "README.md"): "gstack 的安裝、工作流與核心理念入口文件。",
    ("gstack", "SKILL.md"): "用無頭瀏覽器進行網站 QA、互動驗證與證據擷取的基礎技能。",
    ("gstack", "autoplan/SKILL.md"): "自動串接 CEO、設計與工程評審，按原則連續審核方案的總控技能。",
    ("gstack", "benchmark/SKILL.md"): "以瀏覽守護程序建立與比較效能基線的性能檢測技能。",
    ("gstack", "browse/SKILL.md"): "以無頭瀏覽器快速操作頁面、驗證狀態與蒐集 QA 證據的工具技能。",
    ("gstack", "canary/SKILL.md"): "在部署後持續監控線上站點錯誤、效能與畫面異常的金絲雀檢查技能。",
    ("gstack", "careful/SKILL.md"): "為破壞性命令加上事前警告與人工覆核的安全護欄技能。",
    ("gstack", "codex/SKILL.md"): "把 OpenAI Codex 以 review、challenge、consult 三種模式接入工作流的包裝技能。",
    ("gstack", "connect-chrome/SKILL.md"): "連接可視化的真實 Chrome 與側邊欄，讓使用者即時觀看瀏覽操作。",
    ("gstack", "cso/SKILL.md"): "以資安長視角做基礎設施、供應鏈與 AI 風險審計的安全評審技能。",
    ("gstack", "design-consultation/SKILL.md"): "先理解產品再研究競品並提出完整設計系統方案的設計顧問技能。",
    ("gstack", "design-review/SKILL.md"): "以設計師視角檢查並修正 UI 一致性、節奏與互動品質的設計 QA 技能。",
    ("gstack", "design-shotgun/SKILL.md"): "一次產生多個設計變體、比較並收斂偏好的設計探索技能。",
    ("gstack", "document-release/SKILL.md"): "在發版後同步更新 README、架構與變更記錄等文件的文件收尾技能。",
    ("gstack", "freeze/SKILL.md"): "把本次會話的可編輯範圍鎖定在指定目錄的編輯邊界技能。",
    ("gstack", "gstack-upgrade/SKILL.md"): "將 gstack 升級到最新版本並處理升級提示的維護技能。",
    ("gstack", "guard/SKILL.md"): "同時啟用破壞性命令警告與目錄凍結的全量安全模式技能。",
    ("gstack", "investigate/SKILL.md"): "以根因調查、模式分析、假設驗證到實作的流程來系統化除錯。",
    ("gstack", "land-and-deploy/SKILL.md"): "接手 PR 合併、等待部署並做生產環境驗證的落地部署技能。",
    ("gstack", "learn/SKILL.md"): "管理專案長期 learnings，支援查詢、修剪與匯出的知識累積技能。",
    ("gstack", "office-hours/SKILL.md"): "用 YC office hours 與 design partner 心法逼出真需求與更強產品方向的策略技能。",
    ("gstack", "plan-ceo-review/SKILL.md"): "用 CEO 或創辦人視角重新審視計畫範圍、願景與 10 星產品機會。",
    ("gstack", "plan-design-review/SKILL.md"): "從設計師角度逐維度評估計畫，指出把體驗做成 10 分的方法。",
    ("gstack", "plan-eng-review/SKILL.md"): "以工程主管視角鎖定架構、資料流、邊界條件與測試覆蓋的計畫評審技能。",
    ("gstack", "qa/SKILL.md"): "系統化測試網站後直接修 bug、逐項驗證並提交原子修復的 QA 技能。",
    ("gstack", "qa-only/SKILL.md"): "只做網站 QA 測試與結構化報告、不直接改碼的報告型 QA 技能。",
    ("gstack", "retro/SKILL.md"): "分析提交歷史、工作模式與品質指標，形成週期性工程 retrospective。",
    ("gstack", "review/SKILL.md"): "在合併前對 diff 做結構性風險審查並盡可能先修問題的 PR review 技能。",
    ("gstack", "setup-browser-cookies/SKILL.md"): "把真實 Chromium 的 cookies 匯入 headless 瀏覽 session 的登入前置技能。",
    ("gstack", "setup-deploy/SKILL.md"): "偵測部署平台與健康檢查資訊，為 land-and-deploy 建立部署設定。",
    ("gstack", "ship/SKILL.md"): "從合併 base、跑測試、更新版本到推分支建 PR 的完整出貨技能。",
    ("gstack", "unfreeze/SKILL.md"): "解除 freeze 設定，重新允許跨目錄編輯。",
    ("superpowers", "README.md"): "superpowers 的安裝、基本工作流、哲學與技能總覽入口。",
    ("superpowers", "skills/brainstorming/SKILL.md"): "在任何創作或功能實作前先釐清意圖、需求與設計的前置腦暴技能。",
    ("superpowers", "skills/dispatching-parallel-agents/SKILL.md"): "把彼此獨立的任務拆給多個 agent 並行處理的調度技能。",
    ("superpowers", "skills/executing-plans/SKILL.md"): "依既有實作計畫逐步施工、檢查與回報進度的執行技能。",
    ("superpowers", "skills/finishing-a-development-branch/SKILL.md"): "在開發完成後結構化決定合併、提 PR 或清理分支的收尾技能。",
    ("superpowers", "skills/receiving-code-review/SKILL.md"): "收到 code review 後先驗證、再回應與實作，而不是表演式同意的技能。",
    ("superpowers", "skills/requesting-code-review/SKILL.md"): "在完成主要工作後有條理地請求 code review 的協作技能。",
    ("superpowers", "skills/subagent-driven-development/SKILL.md"): "在同一會話內用子代理執行獨立實作子任務的開發技能。",
    ("superpowers", "skills/systematic-debugging/SKILL.md"): "遇到 bug 時先找根因、再提修復方案的系統化除錯技能。",
    ("superpowers", "skills/test-driven-development/SKILL.md"): "在寫實作前先寫失敗測試，再按 red-green-refactor 前進的 TDD 技能。",
    ("superpowers", "skills/using-git-worktrees/SKILL.md"): "用 git worktree 建立隔離工作目錄，安全展開多線開發。",
    ("superpowers", "skills/using-superpowers/SKILL.md"): "在任何對話開始時先找對 skill 並嚴格遵守技能優先級的總入口技能。",
    ("superpowers", "skills/verification-before-completion/SKILL.md"): "在宣稱完成前先跑驗證並以證據支撐結論的收尾技能。",
    ("superpowers", "skills/writing-plans/SKILL.md"): "在動手前先把多步驟工作拆成可執行計畫的規劃技能。",
    ("superpowers", "skills/writing-skills/SKILL.md"): "建立、測試與維護技能文件本身的技能設計技能。",
}

CATEGORY_OVERRIDES = {
    ("gstack", "AGENTS.md"): ["工具程序型", "元技能型"],
    ("gstack", "CLAUDE.md"): ["工具程序型", "流程型", "元技能型"],
    ("gstack", "README.md"): ["工具程序型", "流程型"],
    ("gstack", "SKILL.md"): ["工具程序型", "流程型"],
    ("gstack", "autoplan/SKILL.md"): ["審查型", "流程型", "元技能型"],
    ("gstack", "benchmark/SKILL.md"): ["審查型", "工具程序型", "流程型"],
    ("gstack", "browse/SKILL.md"): ["工具程序型"],
    ("gstack", "canary/SKILL.md"): ["審查型", "流程型", "工具程序型"],
    ("gstack", "careful/SKILL.md"): ["工具程序型"],
    ("gstack", "codex/SKILL.md"): ["審查型", "工具程序型", "流程型"],
    ("gstack", "connect-chrome/SKILL.md"): ["工具程序型", "流程型"],
    ("gstack", "cso/SKILL.md"): ["思考框架型", "審查型", "人格型"],
    ("gstack", "design-consultation/SKILL.md"): ["思考框架型", "流程型", "人格型"],
    ("gstack", "design-review/SKILL.md"): ["審查型", "流程型", "人格型"],
    ("gstack", "design-shotgun/SKILL.md"): ["思考框架型", "流程型"],
    ("gstack", "document-release/SKILL.md"): ["工具程序型", "流程型"],
    ("gstack", "freeze/SKILL.md"): ["工具程序型"],
    ("gstack", "gstack-upgrade/SKILL.md"): ["工具程序型", "流程型"],
    ("gstack", "guard/SKILL.md"): ["工具程序型", "流程型"],
    ("gstack", "investigate/SKILL.md"): ["思考框架型", "流程型"],
    ("gstack", "land-and-deploy/SKILL.md"): ["審查型", "工具程序型", "流程型"],
    ("gstack", "learn/SKILL.md"): ["工具程序型", "流程型"],
    ("gstack", "office-hours/SKILL.md"): ["思考框架型", "人格型"],
    ("gstack", "plan-ceo-review/SKILL.md"): ["思考框架型", "審查型", "人格型", "流程型"],
    ("gstack", "plan-design-review/SKILL.md"): ["思考框架型", "審查型", "人格型", "流程型"],
    ("gstack", "plan-eng-review/SKILL.md"): ["思考框架型", "審查型", "人格型", "流程型"],
    ("gstack", "qa/SKILL.md"): ["審查型", "工具程序型", "流程型"],
    ("gstack", "qa-only/SKILL.md"): ["審查型", "流程型"],
    ("gstack", "retro/SKILL.md"): ["思考框架型", "審查型", "流程型"],
    ("gstack", "review/SKILL.md"): ["思考框架型", "審查型", "流程型"],
    ("gstack", "setup-browser-cookies/SKILL.md"): ["工具程序型"],
    ("gstack", "setup-deploy/SKILL.md"): ["工具程序型", "流程型"],
    ("gstack", "ship/SKILL.md"): ["審查型", "工具程序型", "流程型"],
    ("gstack", "unfreeze/SKILL.md"): ["工具程序型"],
    ("superpowers", "README.md"): ["元技能型", "流程型"],
    ("superpowers", "skills/brainstorming/SKILL.md"): ["思考框架型", "流程型"],
    ("superpowers", "skills/dispatching-parallel-agents/SKILL.md"): ["工具程序型", "流程型", "元技能型"],
    ("superpowers", "skills/executing-plans/SKILL.md"): ["工具程序型", "流程型"],
    ("superpowers", "skills/finishing-a-development-branch/SKILL.md"): ["工具程序型", "流程型"],
    ("superpowers", "skills/receiving-code-review/SKILL.md"): ["思考框架型", "審查型", "流程型"],
    ("superpowers", "skills/requesting-code-review/SKILL.md"): ["工具程序型", "流程型"],
    ("superpowers", "skills/subagent-driven-development/SKILL.md"): ["工具程序型", "流程型", "元技能型"],
    ("superpowers", "skills/systematic-debugging/SKILL.md"): ["思考框架型", "流程型"],
    ("superpowers", "skills/test-driven-development/SKILL.md"): ["思考框架型", "流程型"],
    ("superpowers", "skills/using-git-worktrees/SKILL.md"): ["工具程序型", "流程型"],
    ("superpowers", "skills/using-superpowers/SKILL.md"): ["流程型", "元技能型"],
    ("superpowers", "skills/verification-before-completion/SKILL.md"): ["思考框架型", "審查型", "流程型"],
    ("superpowers", "skills/writing-plans/SKILL.md"): ["思考框架型", "流程型"],
    ("superpowers", "skills/writing-skills/SKILL.md"): ["思考框架型", "流程型", "元技能型"],
}

CATEGORY_ORDER = ["思考框架型", "審查型", "工具程序型", "人格型", "流程型", "元技能型"]

DOMAIN_OVERRIDES = {
    ("gstack", "AGENTS.md"): ["meta"],
    ("gstack", "CLAUDE.md"): ["meta"],
    ("gstack", "README.md"): ["meta", "shipping"],
    ("gstack", "SKILL.md"): ["browser", "review"],
    ("gstack", "autoplan/SKILL.md"): ["planning", "review", "meta"],
    ("gstack", "benchmark/SKILL.md"): ["performance", "browser", "review"],
    ("gstack", "browse/SKILL.md"): ["browser"],
    ("gstack", "canary/SKILL.md"): ["performance", "browser", "shipping", "review"],
    ("gstack", "careful/SKILL.md"): ["safety"],
    ("gstack", "codex/SKILL.md"): ["review", "meta"],
    ("gstack", "connect-chrome/SKILL.md"): ["browser"],
    ("gstack", "cso/SKILL.md"): ["safety", "review"],
    ("gstack", "design-consultation/SKILL.md"): ["design", "planning"],
    ("gstack", "design-review/SKILL.md"): ["design", "review"],
    ("gstack", "design-shotgun/SKILL.md"): ["design", "planning"],
    ("gstack", "document-release/SKILL.md"): ["shipping", "meta"],
    ("gstack", "freeze/SKILL.md"): ["safety"],
    ("gstack", "gstack-upgrade/SKILL.md"): ["meta", "shipping"],
    ("gstack", "guard/SKILL.md"): ["safety"],
    ("gstack", "investigate/SKILL.md"): ["debugging"],
    ("gstack", "land-and-deploy/SKILL.md"): ["shipping", "review", "browser"],
    ("gstack", "learn/SKILL.md"): ["learning", "meta"],
    ("gstack", "office-hours/SKILL.md"): ["planning", "design"],
    ("gstack", "plan-ceo-review/SKILL.md"): ["planning", "review"],
    ("gstack", "plan-design-review/SKILL.md"): ["planning", "review", "design"],
    ("gstack", "plan-eng-review/SKILL.md"): ["planning", "review", "testing"],
    ("gstack", "qa/SKILL.md"): ["review", "browser", "debugging", "testing"],
    ("gstack", "qa-only/SKILL.md"): ["review", "browser", "debugging"],
    ("gstack", "retro/SKILL.md"): ["learning", "review"],
    ("gstack", "review/SKILL.md"): ["review", "shipping", "testing"],
    ("gstack", "setup-browser-cookies/SKILL.md"): ["browser"],
    ("gstack", "setup-deploy/SKILL.md"): ["shipping"],
    ("gstack", "ship/SKILL.md"): ["shipping", "review", "testing"],
    ("gstack", "unfreeze/SKILL.md"): ["safety"],
    ("superpowers", "README.md"): ["meta"],
    ("superpowers", "skills/brainstorming/SKILL.md"): ["planning", "design"],
    ("superpowers", "skills/dispatching-parallel-agents/SKILL.md"): ["parallel", "meta"],
    ("superpowers", "skills/executing-plans/SKILL.md"): ["planning"],
    ("superpowers", "skills/finishing-a-development-branch/SKILL.md"): ["shipping"],
    ("superpowers", "skills/receiving-code-review/SKILL.md"): ["review"],
    ("superpowers", "skills/requesting-code-review/SKILL.md"): ["review"],
    ("superpowers", "skills/subagent-driven-development/SKILL.md"): ["parallel", "meta", "planning"],
    ("superpowers", "skills/systematic-debugging/SKILL.md"): ["debugging"],
    ("superpowers", "skills/test-driven-development/SKILL.md"): ["testing", "debugging"],
    ("superpowers", "skills/using-git-worktrees/SKILL.md"): ["parallel", "shipping"],
    ("superpowers", "skills/using-superpowers/SKILL.md"): ["meta"],
    ("superpowers", "skills/verification-before-completion/SKILL.md"): ["review", "testing", "shipping"],
    ("superpowers", "skills/writing-plans/SKILL.md"): ["planning"],
    ("superpowers", "skills/writing-skills/SKILL.md"): ["meta", "planning", "testing"],
}

KNOWLEDGE_TAGS = ["商業模式", "融資", "行銷", "產品", "技術", "法規", "團隊"]

KNOWLEDGE_KEYWORDS = {
    "商業模式": ["business model", "pricing", "revenue", "monetization", "positioning"],
    "融資": ["fundraising", "investor", "venture", "seed", "series a", "raising money"],
    "行銷": ["marketing", "sales", "growth", "brand", "metrics", "distribution"],
    "產品": ["product", "mvp", "customer", "user", "startup ideas", "roadmap"],
    "技術": ["engineering", "technology", "software", "cto", "technical", "stack"],
    "法規": ["legal", "compliance", "policy", "gdpr", "regulation"],
    "團隊": ["leadership", "team", "hiring", "people", "culture", "management"],
}


@dataclass
class Section:
    heading: str
    lines: list[str]


@dataclass
class Card:
    repo: str
    relpath: str
    file_type: str
    title: str
    output_name: str
    description: str
    full_text: str
    body_lines: list[str]
    sections: list[Section]
    one_liner: str
    categories: list[str]
    traits: list[str]
    domains: list[str]
    framework_lines: list[str]
    must_lines: list[str]
    forbid_lines: list[str]
    question_lines: list[str]
    review_lines: list[str]
    format_lines: list[str]
    applicable_lines: list[str]
    quote_lines: list[str]
    related: list[tuple[str, str, str, str]]
    source_type: str
    card_kind: str
    language: str
    canonical_group: str
    content_hash: str
    knowledge_tags: list[str] = field(default_factory=list)
    output_rel_path: str = ""

    @property
    def card_id(self) -> str:
        return f"{self.repo}:{self.relpath}:{self.title}:{self.card_kind}"


def ensure_dirs() -> None:
    REPOS_ROOT.mkdir(parents=True, exist_ok=True)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    RAW_CARDS_ROOT.mkdir(parents=True, exist_ok=True)


def clone_or_pull(name: str, url: str) -> bool:
    target = REPOS_ROOT / name
    try:
        if target.exists():
            subprocess.run(
                ["git", "-C", str(target), "pull", "--ff-only"],
                check=True,
                capture_output=True,
                text=True,
            )
        else:
            subprocess.run(
                ["git", "clone", "--depth", "1", url, str(target)],
                check=True,
                capture_output=True,
                text=True,
            )
        return True
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else str(exc)
        print(f"[ERROR] {name}: {stderr}")
        return False


def normalize_text(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def read_text(path: Path) -> str:
    return normalize_text(path.read_text(encoding="utf-8", errors="replace")).replace("\ufeff", "")


def is_translation_path(relpath: str) -> bool:
    rel_lower = relpath.lower()
    return any(segment in rel_lower for segment in LANG_MAP)


def matches_skip_pattern(relpath: str, repo_config: dict) -> bool:
    policy = repo_config.get("extract_policy", {})
    skip_patterns = [pattern.lower() for pattern in policy.get("skip_patterns", [])]
    rel_lower = relpath.lower()
    if not skip_patterns:
        return False
    if policy.get("skip_translations") and is_translation_path(relpath):
        return False
    return any(pattern in rel_lower for pattern in skip_patterns)


def iter_target_files(repo_config: dict, base: Path) -> Iterable[tuple[str, Path]]:
    scan_targets = {name.lower() for name in repo_config.get("scan_targets", ["SKILL.md"])}
    scan_root_readme = repo_config.get("scan_root_readme", False)
    max_readme_depth = repo_config.get("max_readme_depth", 1)

    for path in sorted(base.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(base).as_posix()
        if any(part in EXCLUDED_PARTS for part in rel.split("/")):
            continue
        if matches_skip_pattern(rel, repo_config):
            continue

        name = path.name.lower()
        depth = len(rel.split("/"))
        if name in scan_targets:
            yield rel, path
        elif name == ROOT_README.lower() and scan_root_readme and depth <= (max_readme_depth + 1):
            yield rel, path


def parse_frontmatter(text: str) -> tuple[dict[str, str], str]:
    if not text.startswith("---\n"):
        return {}, text

    parts = text.split("---\n", 2)
    if len(parts) < 3:
        return {}, text

    raw = parts[1]
    body = parts[2]
    meta: dict[str, str] = {}
    lines = raw.splitlines()
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        block = re.match(r"^([A-Za-z0-9_-]+):\s*\|$", line)
        if block:
            key = block.group(1)
            idx += 1
            values = []
            while idx < len(lines) and (lines[idx].startswith("  ") or not lines[idx].strip()):
                values.append(lines[idx].strip())
                idx += 1
            meta[key] = "\n".join(value for value in values if value)
            continue
        inline = re.match(r"^([A-Za-z0-9_-]+):\s*(.+)$", line)
        if inline:
            meta[inline.group(1)] = inline.group(2).strip()
        idx += 1
    return meta, body


def fallback_description(body: str) -> str:
    lines = [line.strip() for line in body.splitlines()]
    chunks = []
    buffer = []
    in_code = False
    for line in lines:
        if line.startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        if not line:
            if buffer:
                chunks.append(" ".join(buffer))
                buffer = []
            continue
        if line.startswith("#") or line.startswith("<!--") or (line.startswith("<") and line.endswith(">")):
            continue
        buffer.append(line)
        if len(" ".join(buffer)) > 240:
            break
    if buffer:
        chunks.append(" ".join(buffer))
    return chunks[0] if chunks else ""


def parse_sections(body: str) -> list[Section]:
    sections: list[Section] = []
    current_heading = "__ROOT__"
    current_lines: list[str] = []
    for raw_line in body.splitlines():
        if raw_line.startswith("## "):
            sections.append(Section(current_heading, current_lines))
            current_heading = raw_line[3:].strip()
            current_lines = []
        else:
            current_lines.append(raw_line.rstrip())
    sections.append(Section(current_heading, current_lines))
    return sections


def derive_title(relpath: str) -> str:
    if relpath.endswith("/SKILL.md"):
        return relpath.split("/")[-2]
    return Path(relpath).stem


def output_name_for(relpath: str) -> str:
    parts = relpath.split("/")
    if relpath == "README.md":
        return "_root-readme.md"
    if relpath == "CLAUDE.md":
        return "_root-claude.md"
    if relpath == "AGENTS.md":
        return "_root-agents.md"
    if relpath == "SKILL.md":
        return "_root-skill.md"
    if parts[-1] == "SKILL.md":
        return f"{parts[-2]}.md"
    return f"{Path(relpath).stem.lower()}.md"


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "card"


def output_name_for_knowledge(section_title: str) -> str:
    return f"{slugify(section_title)}.md"


def clean_line(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip())


def interesting_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith("```") or stripped.startswith("<!--"):
        return False
    if stripped.startswith("---"):
        return False
    if len(stripped) < 3:
        return False
    if re.fullmatch(r"[#>*\-\s]+", stripped):
        return False
    return True


def dedupe_keep_order(lines: Iterable[str]) -> list[str]:
    seen = set()
    output = []
    for line in lines:
        key = line.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(line)
    return output


def lines_from_section(section: Section) -> list[str]:
    results = []
    in_code = False
    for raw in section.lines:
        if raw.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        stripped = raw.strip()
        if not interesting_line(stripped):
            continue
        results.append(clean_line(stripped))
    return results


def extract_key_lines(section: Section, limit: int) -> list[str]:
    candidates = []
    for line in lines_from_section(section):
        if (
            re.match(r"^(\d+\.\s|[-*]\s)", line)
            or line.startswith("**")
            or line.endswith(":")
            or "MUST" in line
            or "NEVER" in line
            or "ALWAYS" in line
            or "Question:" in line
            or "Options:" in line
        ):
            candidates.append(line)
    if not candidates:
        candidates = lines_from_section(section)
    return candidates[:limit]


def collect_framework_lines(sections: list[Section]) -> list[str]:
    picked: list[str] = []
    for section in sections:
        heading_lower = section.heading.lower()
        if section.heading == "__ROOT__":
            continue
        if any(keyword in heading_lower for keyword in FRAMEWORK_HEADING_KEYWORDS):
            picked.append(f"[{section.heading}]")
            picked.extend(extract_key_lines(section, 6))
        if len(picked) >= 18:
            break
    return dedupe_keep_order(picked)[:18]


def collect_behavior_lines(lines: list[str]) -> tuple[list[str], list[str]]:
    must_patterns = ("MUST", "ALWAYS", "REQUIRED", "Before ", "Use AskUserQuestion", "Only run", "Ask the user")
    forbid_patterns = ("NEVER", "DO NOT", "don't", "Don't", "cannot", "not optional", "blocked", "skip this", "no fixes without")
    must_lines = []
    forbid_lines = []
    for line in lines:
        if any(token in line for token in must_patterns):
            must_lines.append(line)
        if any(token in line for token in forbid_patterns):
            forbid_lines.append(line)
    return dedupe_keep_order(must_lines)[:12], dedupe_keep_order(forbid_lines)[:12]


def ensure_framework_coverage(sections: list[Section], framework_lines: list[str]) -> list[str]:
    existing = dedupe_keep_order(framework_lines)
    additions: list[str] = []
    for keyword in ("iron law", "phase 4"):
        if any(keyword in line.lower() for line in existing):
            continue
        for section in sections:
            if keyword in section.heading.lower():
                additions.append(f"[{section.heading}]")
                additions.extend(extract_key_lines(section, 3))
                break

    if not additions:
        return existing[:18]

    additions = dedupe_keep_order(additions)
    budget = max(0, 18 - len(additions))
    return dedupe_keep_order(existing[:budget] + additions)[:18]


def ensure_behavior_coverage(lines: list[str], must_lines: list[str], forbid_lines: list[str]) -> tuple[list[str], list[str]]:
    must_priority = [
        line
        for line in lines
        if re.search(r"\b(MUST|ALWAYS|REQUIRED)\b", line) or line.startswith("Before ") or line.startswith("Only run")
    ]
    forbid_priority = [
        line
        for line in lines
        if re.search(r"\b(NEVER|Never)\b", line) or "DO NOT" in line or "Do not" in line or "don't" in line or "Don't" in line
    ]
    merged_must = dedupe_keep_order(must_priority + must_lines)[:12]
    merged_forbid = dedupe_keep_order(forbid_priority + forbid_lines)[:12]
    return merged_must, merged_forbid


def collect_question_lines(sections: list[Section]) -> list[str]:
    picked = []
    for section in sections:
        heading_lower = section.heading.lower()
        if any(keyword in heading_lower for keyword in QUESTION_HEADING_KEYWORDS):
            picked.append(f"[{section.heading}]")
            for line in lines_from_section(section):
                if "Question:" in line or "Options:" in line or line.endswith("?") or "AskUserQuestion" in line:
                    picked.append(line)
        for line in lines_from_section(section):
            if len(picked) >= 12:
                break
            if line.endswith("?") and len(line) < 220:
                picked.append(line)
        if len(picked) >= 12:
            break
    return dedupe_keep_order(picked)[:12]


def split_description_phrases(description: str) -> list[str]:
    cleaned = re.sub(r"\s+", " ", description).strip()
    if not cleaned:
        return []
    pieces = re.split(r";|\. ", cleaned)
    results = []
    for piece in pieces:
        piece = piece.strip(" .")
        if not piece:
            continue
        if len(piece) > 220:
            piece = piece[:217].rstrip() + "..."
        results.append(piece)
    return results


def collect_review_lines(description: str, sections: list[Section], categories: list[str]) -> list[str]:
    if "審查型" not in categories:
        return []
    picked = []
    for section in sections:
        heading_lower = section.heading.lower()
        if any(keyword in heading_lower for keyword in REVIEW_HEADING_KEYWORDS):
            picked.append(f"[{section.heading}]")
            picked.extend(extract_key_lines(section, 6))
        if len(picked) >= 16:
            break
    if len(picked) < 4:
        for piece in split_description_phrases(description):
            lower = piece.lower()
            if any(token in lower for token in ("review", "audit", "qa", "analy", "find", "score", "check", "coverage")):
                picked.append(piece)
    return dedupe_keep_order(picked)[:16]


def collect_format_lines(sections: list[Section]) -> list[str]:
    picked = []
    for section in sections:
        heading_lower = section.heading.lower()
        if any(keyword in heading_lower for keyword in FORMAT_HEADING_KEYWORDS):
            picked.append(f"[{section.heading}]")
            picked.extend(extract_key_lines(section, 6))
        if len(picked) >= 14:
            break
    return dedupe_keep_order(picked)[:14]


def collect_quote_lines(description: str, sections: list[Section], line_count: int) -> list[str]:
    max_quotes = 10
    if line_count > 500:
        max_quotes = 16
    if line_count > 1200:
        max_quotes = 20

    picked = []
    for piece in split_description_phrases(description):
        picked.append(piece)

    for section in sections:
        if section.heading == "__ROOT__":
            continue
        picked.append(section.heading)
        picked.extend(extract_key_lines(section, 4))
        if len(picked) >= max_quotes * 2:
            break

    cleaned = []
    for line in dedupe_keep_order(picked):
        candidate = line.strip()
        if not candidate:
            continue
        if candidate.startswith("[") and candidate.endswith("]"):
            continue
        if len(candidate) > 260:
            candidate = candidate[:257].rstrip() + "..."
        cleaned.append(candidate.lstrip("> ").strip())
    return cleaned[:max_quotes]


def infer_categories(repo: str, relpath: str, description: str, body: str) -> list[str]:
    override = CATEGORY_OVERRIDES.get((repo, relpath))
    if override:
        return override

    text = f"{relpath} {description} {body}".lower()
    categories = []
    if any(token in text for token in ("principle", "framework", "pattern", "philosophy", "think", "brainstorm")):
        categories.append("思考框架型")
    if any(token in text for token in ("review", "audit", "qa", "checklist", "rubric", "verification")):
        categories.append("審查型")
    if any(token in text for token in ("setup", "command", "hook", "import", "connect", "worktree", "tool")):
        categories.append("工具程序型")
    if any(token in text for token in ("ceo", "founder", "designer", "chief security officer", "office hours", "voice")):
        categories.append("人格型")
    if any(token in text for token in ("workflow", "step", "steps", "phase", "process", "red-green-refactor")):
        categories.append("流程型")
    if any(token in text for token in ("skill", "skills", "agents", "claude", "superpowers")):
        categories.append("元技能型")
    return categories or ["工具程序型"]


def infer_traits(relpath: str, description: str, categories: list[str]) -> list[str]:
    text = f"{relpath} {description}".lower()
    traits = []
    if "審查型" in categories:
        traits.extend(["結構化", "批判性", "風險敏感"])
    if "思考框架型" in categories:
        traits.extend(["框架導向", "第一性原理", "推理導向"])
    if "流程型" in categories:
        traits.extend(["流程紀律", "完成導向"])
    if "工具程序型" in categories:
        traits.extend(["務實", "操作導向"])
    if "人格型" in categories:
        traits.extend(["角色鮮明", "觀點強烈"])
    if "元技能型" in categories:
        traits.extend(["規範導向", "元認知"])
    if any(token in text for token in ("debug", "root cause", "verification", "tdd", "test")):
        traits.extend(["證據導向", "懷疑論"])
    if any(token in text for token in ("design", "designer", "typography", "visual")):
        traits.extend(["品味導向", "體驗敏感"])
    if any(token in text for token in ("ceo", "founder", "office hours", "scope", "ambition")):
        traits.extend(["挑戰性", "野心導向", "產品導向"])
    if any(token in text for token in ("security", "safety", "careful", "guard", "freeze")):
        traits.extend(["謹慎", "防呆導向"])
    if any(token in text for token in ("parallel", "subagent", "worktree")):
        traits.extend(["協作導向", "拆解導向"])
    if any(token in text for token in ("learn", "retro", "trend")):
        traits.extend(["反思性", "累積導向"])
    return dedupe_keep_order(traits)[:10]


def infer_domains(repo: str, relpath: str, description: str, headings: list[str]) -> list[str]:
    override = DOMAIN_OVERRIDES.get((repo, relpath))
    if override:
        return override
    text = " ".join([relpath, description, *headings]).lower()
    domains = []
    for domain, keywords in DOMAIN_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            domains.append(domain)
    return domains


def extract_triggers(description: str) -> list[str]:
    triggers = re.findall(r'"([^"]+)"', description)
    return dedupe_keep_order(triggers)


def build_applicable_lines(one_liner: str, description: str) -> list[str]:
    triggers = extract_triggers(description)
    lines = [f"- 適合在需要「{one_liner.rstrip('。')}」的工作階段使用。"]
    if triggers:
        preview = " / ".join(triggers[:8])
        lines.append(f"- 常見觸發語句：{preview}")
    lower = description.lower()
    if "proactively suggest" in lower:
        lines.append("- 也適合在使用者尚未明講，但上下文已顯示相同需求時主動建議使用。")
    if any(token in lower for token in ("review", "audit", "qa", "verification")):
        lines.append("- 常出現在審查、驗證、合併前檢查或上線後回看階段。")
    elif any(token in lower for token in ("plan", "brainstorm", "scope", "requirements")):
        lines.append("- 常出現在規劃、定義問題、確認範圍或決策前討論階段。")
    elif any(token in lower for token in ("deploy", "release", "ship", "branch")):
        lines.append("- 常出現在收尾、發布、分支整理或部署交接階段。")
    elif any(token in lower for token in ("setup", "connect", "import", "worktree")):
        lines.append("- 常出現在正式施工前的環境準備與工具接線階段。")
    return lines[:4]


def tokenize(text: str) -> set[str]:
    text = text.lower()
    tokens = re.findall(r"[a-z][a-z0-9-]{2,}", text)
    return {token for token in tokens if token not in SIMILARITY_STOPWORDS}


def related_reason(shared_domains: set[str], shared_categories: set[str], shared_tokens: set[str]) -> str:
    bits = []
    if shared_domains:
        bits.append("共享領域：" + ", ".join(sorted(shared_domains)[:3]))
    if shared_categories:
        ordered = sorted(shared_categories, key=CATEGORY_ORDER.index)
        bits.append("共享分類：" + "、".join(ordered))
    if shared_tokens:
        bits.append("共同關鍵詞：" + ", ".join(sorted(shared_tokens)[:4]))
    return "；".join(bits) if bits else "主題與流程高度接近"


def relation_type(shared_domains: set[str], shared_categories: set[str]) -> str:
    signal_domains = shared_domains - {"meta"}
    if len(signal_domains) >= 1 or {"審查型", "流程型"}.issubset(shared_categories) or {"思考框架型", "流程型"}.issubset(shared_categories):
        return "相似"
    return "互補"


def markdown_escape_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", "<br>")


def format_checkboxes(categories: list[str]) -> str:
    lines = []
    selected = set(categories)
    for category in CATEGORY_ORDER:
        mark = "x" if category in selected else " "
        lines.append(f"- [{mark}] {category}")
    return "\n".join(lines)


def format_knowledge_checkboxes(tags: list[str]) -> str:
    selected = set(tags)
    return "\n".join(f"- [{'x' if tag in selected else ' '}] {tag}" for tag in KNOWLEDGE_TAGS)


def strip_markdown_for_excerpt(text: str) -> str:
    clean = re.sub(r"```.*?```", " ", text, flags=re.S)
    clean = re.sub(r"`([^`]*)`", r"\1", clean)
    clean = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", clean)
    clean = re.sub(r"[*_>#]", " ", clean)
    clean = re.sub(r"\s+", " ", clean)
    return clean.strip()


def truncate_text(text: str, limit: int) -> str:
    clean = strip_markdown_for_excerpt(text)
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "…"


def first_sentence(text: str) -> str:
    clean = strip_markdown_for_excerpt(text)
    if not clean:
        return ""
    pieces = re.split(r"(?<=[.!?。！？])\s+", clean)
    for piece in pieces:
        piece = piece.strip()
        if len(piece) >= 12:
            return piece
    return clean


def has_template_one_liner(text: str) -> bool:
    return "專注於" in text or "適合處理" in text


def fallback_one_liner(body: str, description: str) -> str:
    candidate = first_sentence(description) or first_sentence(fallback_description(body)) or truncate_text(body, 80)
    candidate = truncate_text(candidate, 80)
    if has_template_one_liner(candidate):
        candidate = truncate_text(fallback_description(body), 80)
    return candidate or "原始文件的能力說明。"


def choose_one_liner(repo: str, relpath: str, description: str, body: str) -> str:
    manual = ONE_LINERS.get((repo, relpath))
    if manual:
        return manual
    return fallback_one_liner(body, description)


def infer_card_kind(repo: str, relpath: str, file_type: str, body: str) -> str:
    del repo, file_type
    rel_lower = relpath.lower()
    name_lower = Path(relpath).name.lower()

    if any(
        lang_dir in rel_lower
        for lang_dir in ("/ja/", "/ja-jp/", "/ko/", "/ko-kr/", "/pt/", "/pt-br/", "/zh/", "/zh-cn/", "/zh-tw/", "/es/", "/fr/", "/de/")
    ):
        return "translation"

    if name_lower == "readme.md":
        return "overview"

    if name_lower in ("claude.md", "agents.md"):
        behavior_signals = sum(1 for keyword in ("MUST", "NEVER", "ALWAYS", "Iron Law", "DO NOT") if keyword in body)
        if behavior_signals >= 2:
            return "agent"
        return "overview"

    if "template" in rel_lower:
        return "template"

    if name_lower == "skill.md":
        return "skill"

    return "overview"


def infer_language(relpath: str, body: str) -> str:
    del body
    rel_lower = relpath.lower()
    for pattern, language in LANG_MAP.items():
        if pattern in rel_lower:
            return language
    return "en"


def compute_canonical_group(repo: str, title: str, relpath: str) -> str:
    del title
    clean_path = relpath.lower()
    for lang_seg in (
        "/ja-jp/",
        "/ja/",
        "/ko-kr/",
        "/ko/",
        "/pt-br/",
        "/pt/",
        "/zh-cn/",
        "/zh/",
        "/zh-tw/",
        "/es/",
        "/fr/",
        "/de/",
        "docs-ja-jp-",
        "docs-ko-kr-",
        "docs-pt-br-",
        "docs-zh-cn-",
    ):
        clean_path = clean_path.replace(lang_seg, "/")
    return f"{repo}::{clean_path}"


def compute_content_hash(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text[:2000]).strip().lower()
    return hashlib.md5(normalized.encode()).hexdigest()[:12]


def effective_card_kind(repo_config: dict, relpath: str, file_type: str, body: str, source_type: str) -> str:
    if source_type == "knowledge":
        return "knowledge"

    kind = infer_card_kind(repo_config["name"], relpath, file_type, body)
    policy = repo_config.get("extract_policy", {})
    name_lower = file_type.lower()
    language = infer_language(relpath, body)

    if name_lower == "readme.md" and policy.get("readme_as_overview"):
        kind = "overview"
    if name_lower == "claude.md" and policy.get("claude_md_as"):
        kind = policy["claude_md_as"]
    if name_lower == "skill.md" and policy.get("prefer_skill_md"):
        kind = "skill"
    if policy.get("skip_translations") and language != policy.get("canonical_language", "en"):
        kind = "translation"

    return kind


def infer_knowledge_tags(text: str) -> list[str]:
    lowered = text.lower()
    tags = []
    for tag, keywords in KNOWLEDGE_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            tags.append(tag)
    return tags or ["商業模式"]


def split_h2_sections(text: str) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    current_title: str | None = None
    current_lines: list[str] = []

    for line in text.splitlines():
        match = re.match(r"^##\s+(.*\S)\s*$", line)
        if match:
            if current_title is not None:
                sections.append((current_title, "\n".join(current_lines).strip()))
            current_title = match.group(1).strip()
            current_lines = []
        elif current_title is not None:
            current_lines.append(line)

    if current_title is not None:
        sections.append((current_title, "\n".join(current_lines).strip()))

    return sections


def split_top_level_h1_sections(text: str) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    current_title: str | None = None
    current_lines: list[str] = []
    first_h1_seen = False

    for line in text.splitlines():
        match = re.match(r"^#\s+(.*\S)\s*$", line)
        if match:
            title = match.group(1).strip()
            if not first_h1_seen:
                first_h1_seen = True
                continue
            if current_title is not None:
                sections.append((current_title, "\n".join(current_lines).strip()))
            current_title = title
            current_lines = []
        elif current_title is not None:
            current_lines.append(line)

    if current_title is not None:
        sections.append((current_title, "\n".join(current_lines).strip()))

    return sections


def split_knowledge_sections(text: str) -> list[tuple[str, str]]:
    sections = split_h2_sections(text)
    if sections:
        return sections
    return split_top_level_h1_sections(text)


def extract_skill_cards(repo_config: dict, base: Path) -> list[Card]:
    cards: list[Card] = []
    repo = repo_config["name"]

    for relpath, path in iter_target_files(repo_config, base):
        text = read_text(path)
        metadata, body = parse_frontmatter(text)
        description = metadata.get("description") or fallback_description(body)
        sections = parse_sections(body)
        headings = [section.heading for section in sections if section.heading != "__ROOT__"]
        categories = infer_categories(repo, relpath, description, body)
        body_lines = [clean_line(line) for line in body.splitlines() if interesting_line(line)]
        one_liner = choose_one_liner(repo, relpath, description, body)
        traits = infer_traits(relpath, description, categories)
        domains = infer_domains(repo, relpath, description, headings)
        framework_lines = collect_framework_lines(sections)
        must_lines, forbid_lines = collect_behavior_lines(body_lines)
        framework_lines = ensure_framework_coverage(sections, framework_lines)
        must_lines, forbid_lines = ensure_behavior_coverage(body_lines, must_lines, forbid_lines)
        question_lines = collect_question_lines(sections)
        review_lines = collect_review_lines(description, sections, categories)
        format_lines = collect_format_lines(sections)
        applicable_lines = build_applicable_lines(one_liner, description)
        quote_lines = collect_quote_lines(description, sections, len(text.splitlines()))
        file_type = Path(relpath).name
        card_kind = effective_card_kind(repo_config, relpath, file_type, body, "skill")
        language = infer_language(relpath, body)
        canonical_group = compute_canonical_group(repo, derive_title(relpath), relpath)

        cards.append(
            Card(
                repo=repo,
                relpath=relpath,
                file_type=file_type,
                title=derive_title(relpath),
                output_name=output_name_for(relpath),
                description=description,
                full_text=text,
                body_lines=body_lines,
                sections=sections,
                one_liner=one_liner,
                categories=categories,
                traits=traits,
                domains=domains,
                framework_lines=framework_lines,
                must_lines=must_lines,
                forbid_lines=forbid_lines,
                question_lines=question_lines,
                review_lines=review_lines,
                format_lines=format_lines,
                applicable_lines=applicable_lines,
                quote_lines=quote_lines,
                related=[],
                source_type="skill",
                card_kind=card_kind,
                language=language,
                canonical_group=canonical_group,
                content_hash=compute_content_hash(text),
            )
        )

    return cards


def extract_knowledge_cards(repo_config: dict, base: Path) -> list[Card]:
    readme_path = base / "README.md"
    if not readme_path.exists():
        return []

    text = read_text(readme_path)
    cards: list[Card] = []

    for section_title, section_body in split_knowledge_sections(text):
        non_empty = [line for line in section_body.splitlines() if line.strip()]
        if len(non_empty) < 3:
            continue

        relpath = f"README.md#{slugify(section_title)}"
        description = first_sentence(section_body) or truncate_text(section_body, 180)
        sections = parse_sections(section_body)
        categories = infer_categories(repo_config["name"], relpath, description, section_body)
        traits = infer_traits(relpath, description, categories)
        domains = infer_domains(repo_config["name"], relpath, description, [section_title])
        knowledge_tags = infer_knowledge_tags(f"{section_title}\n{section_body}")
        applicable = [
            "- 適合在需要快速建立該主題知識地圖與資源清單時使用。",
            "- 可作為創業、產品、募資、行銷與團隊研究的起點。",
        ]
        body_lines = [clean_line(line) for line in section_body.splitlines() if interesting_line(line)]

        cards.append(
            Card(
                repo=repo_config["name"],
                relpath=relpath,
                file_type="README.md",
                title=section_title,
                output_name=output_name_for_knowledge(section_title),
                description=description,
                full_text=section_body,
                body_lines=body_lines,
                sections=sections,
                one_liner=fallback_one_liner(section_body, description),
                categories=categories,
                traits=traits,
                domains=domains,
                framework_lines=[],
                must_lines=[],
                forbid_lines=[],
                question_lines=[],
                review_lines=[],
                format_lines=[],
                applicable_lines=applicable,
                quote_lines=[],
                related=[],
                source_type="knowledge",
                card_kind="knowledge",
                language="en",
                canonical_group=compute_canonical_group(repo_config["name"], section_title, relpath),
                content_hash=compute_content_hash(section_body),
                knowledge_tags=knowledge_tags,
            )
        )

    return cards


def compute_relations(cards: list[Card]) -> None:
    token_sets = {
        card.card_id: tokenize(
            " ".join(
                [
                    card.title,
                    card.description,
                    " ".join(section.heading for section in card.sections if section.heading != "__ROOT__"),
                ]
            )
        )
        for card in cards
    }

    for card in cards:
        scores = []
        card_tokens = token_sets[card.card_id]
        for other in cards:
            if other.card_id == card.card_id:
                continue
            if other.repo == card.repo:
                continue
            shared_domains = set(card.domains) & set(other.domains)
            shared_categories = set(card.categories) & set(other.categories)
            shared_tokens = card_tokens & token_sets[other.card_id]
            signal_domains = shared_domains - {"meta"}
            score = len(signal_domains) * 6 + len(shared_tokens) * 2 + max(0, len(shared_categories) - 1)
            if "meta" in shared_domains:
                score += 1
            if score <= 0:
                continue
            if not signal_domains and len(shared_tokens) < 2 and len(shared_categories) < 3:
                continue
            scores.append((score, other, shared_domains, shared_categories, shared_tokens))

        scores.sort(key=lambda item: (-item[0], item[1].repo, item[1].title))
        picked = []
        for _, other, shared_domains, shared_categories, shared_tokens in scores:
            picked.append(
                (
                    other.title,
                    other.repo,
                    relation_type(shared_domains, shared_categories),
                    related_reason(shared_domains, shared_categories, shared_tokens),
                )
            )
            if len(picked) >= 4:
                break
        card.related = picked


def build_card_markdown(card: Card) -> str:
    framework = "\n".join(f"- {line}" for line in card.framework_lines) if card.framework_lines else "原文過短，無法提取"
    must_block = "\n".join(f"- {line}" for line in card.must_lines) if card.must_lines else "- 原文過短，無法提取"
    forbid_block = "\n".join(f"- {line}" for line in card.forbid_lines) if card.forbid_lines else "- 原文過短，無法提取"
    question_block = "\n".join(f"- {line}" for line in card.question_lines) if card.question_lines else "無明確提問模板"
    review_block = "\n".join(f"- {line}" for line in card.review_lines) if card.review_lines else "非審查型"
    format_block = "\n".join(f"- {line}" for line in card.format_lines) if card.format_lines else "無特定格式要求"
    applicable_block = "\n".join(card.applicable_lines) if card.applicable_lines else "- 適用場景未明示"
    quote_block = "\n".join(f"> {line}" for line in card.quote_lines) if card.quote_lines else "> 原文過短，無法提取"
    related_block = (
        "\n".join(f"- {name}（{repo}） - {relation} - {reason}" for name, repo, relation, reason in card.related)
        if card.related
        else "- 尚未發現明確關聯"
    )

    return f"""---
## {card.repo} / {card.title}

### 來源
- repo：{card.repo}
- 路徑：{card.relpath}
- 檔案類型：{card.file_type}
- card_kind：{card.card_kind}
- language：{card.language}
- canonical_group：{card.canonical_group}

### 一句話定位
{card.one_liner}

### 核心人格特質
{", ".join(card.traits) if card.traits else "原文過短，無法提取"}

### 核心思考框架
{framework}

### 核心行為規則
必須做
{must_block}

禁止做
{forbid_block}

### 提問方式
{question_block}

### 審查維度
{review_block}

### 輸出格式要求
{format_block}

### 適用場景
{applicable_block}

### 原文精華摘錄
{quote_block}

### 和其他 skill 的潛在關聯
{related_block}

### 分類標記
{format_checkboxes(card.categories)}
---
"""


def render_knowledge_card(card: Card) -> str:
    knowledge_lines = "\n".join(f"- {line}" for line in dedupe_keep_order(card.body_lines)[:16]) or "- 原始段落以清單整理知識主題。"
    applicable = "\n".join(card.applicable_lines) if card.applicable_lines else "- 適合在需要建立主題知識地圖時使用。"
    return f"""---
## {card.repo} / {card.title}

### 來源
- repo：{card.repo}
- 路徑：{card.relpath}
- 類型：知識庫
- card_kind：{card.card_kind}
- language：{card.language}
- canonical_group：{card.canonical_group}

### 主題
{truncate_text(card.full_text, 260)}

### 核心知識
{knowledge_lines}

### 適用場景
{applicable}

### 分類標記
{format_knowledge_checkboxes(card.knowledge_tags)}
---
"""


def choose_unique_output_name(repo: str, output_name: str, used: dict[str, set[str]]) -> str:
    base = Path(output_name).stem
    suffix = Path(output_name).suffix or ".md"
    candidate = output_name
    index = 2
    while candidate in used[repo]:
        candidate = f"{base}-{index}{suffix}"
        index += 1
    used[repo].add(candidate)
    return candidate


def write_raw_cards(cards: list[Card]) -> None:
    RAW_CARDS_ROOT.mkdir(parents=True, exist_ok=True)
    used: dict[str, set[str]] = defaultdict(set)
    for card in sorted(cards, key=lambda item: (item.repo, item.output_name, item.relpath)):
        repo_dir = RAW_CARDS_ROOT / card.repo
        repo_dir.mkdir(parents=True, exist_ok=True)
        filename = choose_unique_output_name(card.repo, card.output_name, used)
        target = repo_dir / filename
        content = render_knowledge_card(card) if card.source_type == "knowledge" else build_card_markdown(card)
        target.write_text(content, encoding="utf-8")
        card.output_rel_path = target.relative_to(OUTPUT_ROOT).as_posix()


def build_index(all_cards: list[Card], main_cards: list[Card], appendix_cards: list[Card], config: dict, dedup_merges: list[dict]) -> str:
    category_counts = Counter()
    for card in main_cards:
        category_counts.update(card.categories)

    repo_counts = defaultdict(lambda: {"all": 0, "main": 0, "appendix": 0})
    main_ids = {card.card_id for card in main_cards}
    appendix_ids = {card.card_id for card in appendix_cards}
    for card in all_cards:
        repo_counts[card.repo]["all"] += 1
        if card.card_id in main_ids:
            repo_counts[card.repo]["main"] += 1
        if card.card_id in appendix_ids:
            repo_counts[card.repo]["appendix"] += 1

    relation_rows = []
    seen_pairs = set()
    for card in main_cards:
        for name, repo, relation, reason in card.related:
            key = tuple(sorted([(card.repo, card.title), (repo, name)])) + (relation,)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            relation_rows.append(
                f"| {markdown_escape_cell(card.title)} | {card.repo} | {markdown_escape_cell(name)} | {repo} | {relation} | {markdown_escape_cell(reason)} |"
            )

    lines = [
        "# 00_總索引",
        "",
        "## 統計",
        f"- 編譯時間：{datetime.now().isoformat(timespec='seconds')}",
        f"- config 版本：{config.get('version', 'unknown')}",
        f"- 總卡片數：{len(all_cards)}",
        f"- 主索引卡片數：{len(main_cards)}",
        f"- 附錄卡片數：{len(appendix_cards)}",
        f"- canonical_group 去重後數量：{len({card.canonical_group for card in all_cards})}",
        f"- 思考框架型數量：{category_counts['思考框架型']}",
        f"- 審查型數量：{category_counts['審查型']}",
        f"- 工具程序型數量：{category_counts['工具程序型']}",
        f"- 人格型數量：{category_counts['人格型']}",
        f"- 流程型數量：{category_counts['流程型']}",
        f"- 元技能型數量：{category_counts['元技能型']}",
        "",
        "## 各 Repo 卡片數",
        "",
        "| Repo | 全部卡片 | 主索引 | 附錄 |",
        "|------|----------|--------|------|",
    ]

    for repo in sorted(repo_counts):
        lines.append(f"| {repo} | {repo_counts[repo]['all']} | {repo_counts[repo]['main']} | {repo_counts[repo]['appendix']} |")

    lines.extend(
        [
            "",
            "## 主索引（skill + agent + knowledge）",
            "",
            "| Repo | 標題 | card_kind | language | 一句話定位 | 分類 | 原始卡片 |",
            "|------|------|-----------|----------|------------|------|----------|",
        ]
    )
    for card in sorted(main_cards, key=lambda item: (item.repo, item.title.lower(), item.relpath.lower())):
        lines.append(
            f"| {card.repo} | {markdown_escape_cell(card.title)} | {card.card_kind} | {card.language} | "
            f"{markdown_escape_cell(card.one_liner)} | {markdown_escape_cell('、'.join(card.categories))} | [link]({card.output_rel_path}) |"
        )

    lines.extend(
        [
            "",
            "## 附錄：其他卡片",
            "",
            "| Repo | 標題 | card_kind | language | 一句話定位 | 原始卡片 |",
            "|------|------|-----------|----------|------------|----------|",
        ]
    )
    for card in sorted(appendix_cards, key=lambda item: (item.repo, item.card_kind, item.title.lower(), item.relpath.lower())):
        lines.append(
            f"| {card.repo} | {markdown_escape_cell(card.title)} | {card.card_kind} | {card.language} | "
            f"{markdown_escape_cell(card.one_liner)} | [link]({card.output_rel_path}) |"
        )

    lines.extend(
        [
            "",
            "## 去重合併",
            "",
            "| canonical_group | merged | kept |",
            "|-----------------|--------|------|",
        ]
    )
    if dedup_merges:
        for item in dedup_merges:
            lines.append(f"| {markdown_escape_cell(item['canonical_group'])} | {item['merged']} | {item['kept']} |")
    else:
        lines.append("| - | 0 | - |")

    lines.extend(
        [
            "",
            "## 跨 Repo 潛在關聯",
            "",
            "| 卡片 A | Repo A | 卡片 B | Repo B | 關係 | 理由 |",
            "|--------|--------|--------|--------|------|------|",
        ]
    )
    if relation_rows:
        lines.extend(relation_rows)
    else:
        lines.append("| - | - | - | - | - | - |")

    return "\n".join(lines) + "\n"


def build_classification(cards: list[Card]) -> str:
    grouped = defaultdict(list)
    for card in cards:
        for category in card.categories:
            grouped[category].append(card)

    lines = ["# 01_分類彙整", "", "> 只收主索引中的 skill / agent / knowledge 卡片。", ""]
    for category in CATEGORY_ORDER:
        lines.extend(
            [
                f"## {category}",
                "",
                "| Repo | 標題 | card_kind | 一句話定位 | 原始卡片 |",
                "|------|------|-----------|------------|----------|",
            ]
        )
        entries = sorted(grouped[category], key=lambda item: (item.repo, item.title.lower()))
        if entries:
            for card in entries:
                lines.append(
                    f"| {card.repo} | {markdown_escape_cell(card.title)} | {card.card_kind} | "
                    f"{markdown_escape_cell(card.one_liner)} | [link]({card.output_rel_path}) |"
                )
        else:
            lines.append("| - | - | - | 無 | - |")
        lines.append("")

    return "\n".join(lines)


def split_note(note: str) -> tuple[str, str]:
    if not note:
        return "工具 / MCP 資源", ""
    parts = re.split(r"[，,；;。]", note, maxsplit=1)
    purpose = parts[0].strip() if parts else note.strip()
    return purpose or note.strip(), note.strip()


def write_tool_list(config: dict) -> None:
    lines = [
        "# 工具清單",
        "",
        "> 以下 repo 為工具/MCP 類型，不含思維框架人格，不拆卡片。",
        "> 僅供參考與安裝使用。",
        "",
        "| 名稱 | GitHub URL | 用途 | 備註 |",
        "|------|-----------|------|------|",
    ]

    for repo in config["repos"]:
        if not repo.get("enabled", True) or repo.get("type") != "tool":
            continue
        purpose, note = split_note(repo.get("note", ""))
        lines.append(f"| {repo['name']} | {repo['url']} | {purpose} | {note} |")

    (OUTPUT_ROOT / "02_工具清單.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def dedupe_main_index(cards: list[Card], config: dict) -> tuple[list[Card], list[dict]]:
    all_groups: dict[str, list[Card]] = defaultdict(list)
    groups: dict[str, list[Card]] = defaultdict(list)
    repo_policies = {repo["name"]: repo.get("extract_policy", {}) for repo in config["repos"]}

    for card in cards:
        all_groups[card.canonical_group].append(card)
        if card.card_kind not in MAIN_CARD_KINDS:
            continue
        groups[card.canonical_group].append(card)

    def sort_key(item: Card) -> tuple[int, int, int, str]:
        policy = repo_policies.get(item.repo, {})
        canonical_language = policy.get("canonical_language", "en")
        kind_priority = {"skill": 3, "agent": 2, "knowledge": 1}.get(item.card_kind, 0)
        return (len(item.full_text), 1 if item.language == canonical_language else 0, kind_priority, item.card_id)

    kept: list[Card] = []
    dedup_merges: list[dict] = []
    for canonical_group, group in all_groups.items():
        if len(group) <= 1:
            continue
        winner = sorted(group, key=sort_key, reverse=True)[0]
        dedup_merges.append(
            {
                "canonical_group": canonical_group,
                "merged": len(group),
                "kept": winner.language,
            }
        )

    for canonical_group, group in groups.items():
        if len(group) == 1:
            kept.append(group[0])
            continue
        winner = sorted(group, key=sort_key, reverse=True)[0]
        kept.append(winner)

    kept.sort(key=lambda item: (item.repo, item.title.lower(), item.relpath.lower()))
    dedup_merges.sort(key=lambda item: item["canonical_group"])
    return kept, dedup_merges


def write_compile_log(
    all_cards: list[Card],
    main_cards: list[Card],
    appendix_cards: list[Card],
    dedup_merges: list[dict],
    errors: list[str],
    clone_results: dict[str, bool],
    config: dict,
) -> None:
    attempted_repos = [
        repo for repo in config["repos"] if repo.get("enabled", True) and repo["type"] not in ("tool", "reference")
    ]
    failed = [repo["name"] for repo in attempted_repos if not clone_results.get(repo["name"], False)]

    size_values = []
    for card in all_cards:
        if card.output_rel_path:
            target = OUTPUT_ROOT / card.output_rel_path
            if target.exists():
                size_values.append(target.stat().st_size)

    cards_by_kind = {kind: 0 for kind in ALL_CARD_KINDS}
    for card in all_cards:
        cards_by_kind[card.card_kind] = cards_by_kind.get(card.card_kind, 0) + 1

    quality_checks = {
        "cards_with_套版_one_liner": sum(1 for card in all_cards if has_template_one_liner(card.one_liner)),
        "cards_with_empty_framework": sum(
            1 for card in all_cards if card.source_type == "skill" and card.card_kind in {"skill", "agent"} and not card.framework_lines
        ),
        "cards_with_empty_behavior": sum(
            1
            for card in all_cards
            if card.source_type == "skill" and card.card_kind in {"skill", "agent"} and not card.must_lines and not card.forbid_lines
        ),
        "average_card_size_bytes": round(sum(size_values) / len(size_values), 2) if size_values else 0,
    }

    compile_log = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "config_version": config.get("version", "unknown"),
        "repos_attempted": len(attempted_repos),
        "repos_succeeded": len(attempted_repos) - len(failed),
        "repos_failed": failed,
        "total_cards": len(all_cards),
        "cards_after_dedup": len({card.canonical_group for card in all_cards}),
        "cards_by_repo": dict(sorted(Counter(card.repo for card in all_cards).items())),
        "cards_by_kind": cards_by_kind,
        "cards_in_main_index": len(main_cards),
        "cards_in_appendix": len(appendix_cards),
        "dedup_merges": dedup_merges,
        "quality_checks": quality_checks,
        "errors": errors,
        "warnings": [],
    }

    (OUTPUT_ROOT / "compile_log.json").write_text(
        json.dumps(compile_log, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def extract_repo_cards(repo_config: dict, base: Path) -> list[Card]:
    if repo_config["type"] == "skill":
        return extract_skill_cards(repo_config, base)
    if repo_config["type"] == "knowledge":
        return extract_knowledge_cards(repo_config, base)
    return []


def write_index_files(all_cards: list[Card], main_cards: list[Card], appendix_cards: list[Card], config: dict, dedup_merges: list[dict]) -> None:
    (OUTPUT_ROOT / "00_總索引.md").write_text(
        build_index(all_cards, main_cards, appendix_cards, config, dedup_merges),
        encoding="utf-8",
    )
    (OUTPUT_ROOT / "01_分類彙整.md").write_text(build_classification(main_cards), encoding="utf-8")


def main() -> None:
    config = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    ensure_dirs()

    print("=== Phase 1: Clone ===")
    clone_results: dict[str, bool] = {}
    for repo in config["repos"]:
        if not repo.get("enabled", True):
            continue
        if repo["type"] in ("tool", "reference"):
            continue
        clone_results[repo["name"]] = clone_or_pull(repo["name"], repo["url"])

    print("=== Phase 2-3: Scan + Extract ===")
    all_cards: list[Card] = []
    errors: list[str] = []
    for repo in config["repos"]:
        if not repo.get("enabled", True):
            continue
        if repo["type"] in ("tool", "reference"):
            continue
        if not clone_results.get(repo["name"], False):
            errors.append(f"{repo['name']}: clone 失敗，跳過掃描")
            continue

        cards = extract_repo_cards(repo, REPOS_ROOT / repo["name"])
        all_cards.extend(cards)
        print(f"  {repo['name']}: {len(cards)} cards")

    print("=== Phase 3.5: Compute Relations ===")
    compute_relations(all_cards)
    main_cards, dedup_merges = dedupe_main_index(all_cards, config)
    appendix_cards = [card for card in all_cards if card.card_kind not in MAIN_CARD_KINDS]

    print("=== Phase 4: Write Output ===")
    write_raw_cards(all_cards)
    write_index_files(all_cards, main_cards, appendix_cards, config, dedup_merges)
    write_tool_list(config)
    write_compile_log(all_cards, main_cards, appendix_cards, dedup_merges, errors, clone_results, config)

    print("\n=== 完成 ===")
    print(f"總卡片數: {len(all_cards)}")
    print(f"主索引卡片數: {len(main_cards)}")
    print(f"附錄卡片數: {len(appendix_cards)}")
    print(f"錯誤數: {len(errors)}")
    for error in errors:
        print(f"  [ERROR] {error}")


if __name__ == "__main__":
    main()
