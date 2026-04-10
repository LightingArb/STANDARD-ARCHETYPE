"""
02_init.py — 專案目錄結構初始化

只負責：
  1. 建立 data/ logs/ 08_snapshot/ _archive/ config/ 等必要目錄
  2. 檢查 Python 版本與必要套件

不做：
  - 不讀不寫任何 CSV
  - 不呼叫任何 API
  - 不清理舊資料
  - 不做語義分析
  - 不做城市資料補值
"""

import logging
import sys
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ============================================================
# 必要目錄結構
# ============================================================

REQUIRED_DIRS = [
    "config",
    "data",
    "logs",
    "logs/01_main",
    "logs/03_market_catalog",
    "logs/04_market_master",
    "_archive",
    "_archive/docs",
    "_archive/legacy",
    # STEP 3: raw data dirs（城市子目錄由 05/06 動態建立）
    "data/raw/D",
    "data/raw/B",
    "data/raw/prices",
    # STEP 3: processed dirs
    "data/processed/forecast_daily_high",
    "data/processed/truth_daily_high",
    "data/processed/error_table",
    # STEP 4: model dirs
    "data/models/empirical",
    "data/models/ou_ar",
    "data/models/quantile_regression",
    # STEP 4: results dirs
    "data/results/probability",
    "data/results/ev_signals",
]

# ============================================================
# 必要套件
# ============================================================

REQUIRED_PACKAGES = ["requests"]

MIN_PYTHON = (3, 9)


# ============================================================
# 主流程
# ============================================================

def check_python_version() -> bool:
    """檢查 Python 版本 >= 3.9"""
    current = sys.version_info[:2]
    if current < MIN_PYTHON:
        log.error(f"Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ 必要，目前: {current[0]}.{current[1]}")
        return False
    log.info(f"Python 版本: {current[0]}.{current[1]} OK")
    return True


def check_packages() -> bool:
    """檢查必要套件是否已安裝"""
    missing = []
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)

    if missing:
        log.error(f"缺少套件: {', '.join(missing)}")
        log.error(f"請執行: pip install {' '.join(missing)}")
        return False

    log.info(f"必要套件檢查通過: {', '.join(REQUIRED_PACKAGES)}")
    return True


def create_directories() -> int:
    """建立所有必要目錄，回傳新建數量"""
    created = 0
    for rel in REQUIRED_DIRS:
        d = PROJ_DIR / rel
        if not d.exists():
            d.mkdir(parents=True, exist_ok=True)
            log.info(f"  建立: {rel}/")
            created += 1
        else:
            log.debug(f"  已存在: {rel}/")
    return created


def run() -> bool:
    """主入口"""
    log.info("=" * 50)
    log.info("02_init: 專案目錄結構初始化")
    log.info("=" * 50)

    # Step 1: Python 版本
    if not check_python_version():
        return False

    # Step 2: 必要套件
    if not check_packages():
        return False

    # Step 3: 目錄結構
    log.info("建立目錄結構...")
    created = create_directories()
    log.info(f"  新建 {created} 個目錄")

    log.info("02_init 完成。")
    return True


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
