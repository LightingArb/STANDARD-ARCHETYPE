"""
tools/send_test_layouts.py — 排版測試工具

直接用 Telegram Bot API 發訊息，讓使用者在手機上確認排版效果。
執行：python tools/send_test_layouts.py
"""

import sys
import time
from pathlib import Path

import requests

PROJ_DIR = Path(__file__).resolve().parent.parent

# ── 讀設定 ────────────────────────────────────────────────────

def load_config():
    cfg = {}
    yaml_path = PROJ_DIR / "config" / "telegram.yaml"
    for line in yaml_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        k, _, v = line.partition(":")
        cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg

config = load_config()
TOKEN = config["bot_token"]
CHAT_ID = "752773419"


def send(text: str, parse_mode: str = "HTML") -> None:
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
    })
    print(f"Status: {resp.status_code}")
    if resp.status_code != 200:
        print(resp.json())
    time.sleep(1)


# ── 排行排版 ──────────────────────────────────────────────────

RANKING_A = """\
<b>價差排名</b>
━━━━━━━━━━━━━━━━━━

1  <b>Paris</b> · 04/09 · 20小時
   <b>23°C</b>  NO
   $0.008 → $0.010 · +20.5% · $71
──────────────────
2  <b>Paris</b> · 04/11 · 2天20小時
   <b>18°C</b>  NO
   $0.120 → $0.139 · +15.7% · $22
──────────────────
3  <b>London</b> · 04/09 · 20小時
   <b>19°C</b>  YES
   $0.020 → $0.023 · +15.2% · $65
──────────────────
4  <b>London</b> · 04/09 · 20小時
   <b>21°C</b>  NO
   $0.589 → $0.668 · +13.4% · $382
──────────────────
5  <b>London</b> · 04/09 · 20小時
   <b>22°C</b>  NO
   $0.780 → $0.880 · +12.8% · $724

                          1 / 3"""

RANKING_B = """\
<b>價差排名</b>

1 · Paris · 04/09 · 20小時
    23°C NO
    $0.008 → $0.010  +20.5%  $71

2 · Paris · 04/11 · 2天20小時
    18°C NO
    $0.120 → $0.139  +15.7%  $22

3 · London · 04/09 · 20小時
    19°C YES
    $0.020 → $0.023  +15.2%  $65

4 · London · 04/09 · 20小時
    21°C NO
    $0.589 → $0.668  +13.4%  $382

5 · London · 04/09 · 20小時
    22°C NO
    $0.780 → $0.880  +12.8%  $724

                          1 / 3"""

RANKING_C = """\
<b>價差排名</b>
━━━━━━━━━━━━━━━━━━

#1  Paris 04/09 (20小時)
    23°C NO | +20.5% | $71
    $0.008 → $0.010
──────────────────
#2  Paris 04/11 (2天20小時)
    18°C NO | +15.7% | $22
    $0.120 → $0.139
──────────────────
#3  London 04/09 (20小時)
    19°C YES | +15.2% | $65
    $0.020 → $0.023
──────────────────
#4  London 04/09 (20小時)
    21°C NO | +13.4% | $382
    $0.589 → $0.668
──────────────────
#5  London 04/09 (20小時)
    22°C NO | +12.8% | $724
    $0.780 → $0.880

                          1 / 3"""

# ── 城市議題頁排版 ────────────────────────────────────────────

CITY_A = """\
<b>Paris — 04/09</b>
結算：20小時
━━━━━━━━━━━━━━━━━━

<b>23°C</b>  NO
$0.008 → $0.010 · +20.5% · $71
──────────────────
<b>22°C</b>  YES
$0.009 → $0.010 · +7.4% · $18
──────────────────
<b>21°C</b>  YES
$0.009 → $0.009 · +2.5% · $12
──────────────────
<b>20°C</b>  YES
$0.006 → $0.006 · +1.2% · $6"""

CITY_B = """\
<b>Paris — 04/09</b>
結算：20小時

23°C NO
$0.008 → $0.010  +20.5%  $71

22°C YES
$0.009 → $0.010  +7.4%  $18

21°C YES
$0.009 → $0.009  +2.5%  $12

20°C YES
$0.006 → $0.006  +1.2%  $6"""

CITY_C = """\
<b>Paris — 04/09</b>
結算：20小時
━━━━━━━━━━━━━━━━━━

23°C NO | +20.5% | $71
$0.008 → $0.010
──────────────────
22°C YES | +7.4% | $18
$0.009 → $0.010
──────────────────
21°C YES | +2.5% | $12
$0.009 → $0.009
──────────────────
20°C YES | +1.2% | $6
$0.006 → $0.006"""


# ── 主程式 ────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== 排行排版測試 ===")
    send("=== 排行排版測試 ===", parse_mode="")
    print("發送方案 A...")
    send(RANKING_A)
    print("發送方案 B...")
    send(RANKING_B)
    print("發送方案 C...")
    send(RANKING_C)

    print("=== 城市議題頁排版測試 ===")
    send("=== 城市議題頁排版測試 ===", parse_mode="")
    print("發送城市 A...")
    send(CITY_A)
    print("發送城市 B...")
    send(CITY_B)
    print("發送城市 C...")
    send(CITY_C)

    print("完成。")
