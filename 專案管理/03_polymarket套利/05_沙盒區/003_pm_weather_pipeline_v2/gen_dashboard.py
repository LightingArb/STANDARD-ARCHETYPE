import csv, json, logging
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

proj = Path(__file__).resolve().parent
now_utc = datetime.now(timezone.utc)

def get_settlement_countdown(market_date, city):
    if not market_date: return ""
    try:
        tz_name = "Europe/London" if city == "London" else "Europe/Paris"
        settle_dt = datetime.strptime(market_date, "%Y-%m-%d").replace(
            hour=23, minute=59, tzinfo=ZoneInfo(tz_name)
        )
        diff = settle_dt - now_utc.astimezone(ZoneInfo(tz_name))
        if diff.total_seconds() <= 0: return "settled"
        days = diff.days
        hours = diff.seconds // 3600
        mins = (diff.seconds % 3600) // 60
        if days > 0: return f"{days}d {hours}h"
        elif hours > 0: return f"{hours}h {mins}m"
        else: return f"{mins}m"
    except Exception as e:
        log.debug(f"Dashboard format error: {e}")
        return "?"

def get_contract_name(r):
    mt = r.get("market_type", "")
    th = r.get("threshold", "")
    rl, rh = r.get("range_low", ""), r.get("range_high", "")
    if mt == "below": return f"Below {th}\u00b0C"
    elif mt == "exact": return f"Exactly {th}\u00b0C"
    elif mt == "higher": return f"Above {th}\u00b0C"
    elif mt == "range": return f"{rl}\u2013{rh}\u00b0C"
    return f"{mt} {th}"

def sf(v):
    try: return float(v)
    except: return None

def fmt_pct(v):
    f = sf(v)
    if f is None: return "&mdash;"
    return f"{f*100:+.1f}%"

def fmt_dollar(v):
    f = sf(v)
    if f is None: return "&mdash;"
    return ("+$" if f > 0 else "&minus;$") + f"{abs(f):.3f}"

def fmt_kelly(v):
    f = sf(v)
    if f is None or f <= 0: return "&mdash;"
    return f"${f:,.0f}"

def fmt_price(v):
    f = sf(v)
    if f is None: return "&mdash;"
    return f"${f:.2f}"

def fmt_prob(v):
    f = sf(v)
    if f is None: return "&mdash;"
    return f"{f*100:.1f}%"

cities_data = {}
for city in ["London", "Paris"]:
    ev_path = proj / "data" / "results" / "ev_signals" / city / "ev_signals.csv"
    if not ev_path.exists():
        cities_data[city] = {"markets":[], "signals":{}, "dates":[], "predicted":"?", "lead":"?", "countdown":"?", "model_info":""}
        continue
    with open(ev_path, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    markets = []
    for r in rows:
        ye, ne = sf(r.get("yes_edge")), sf(r.get("no_edge"))
        yev, nev = sf(r.get("yes_ev")), sf(r.get("no_ev"))
        signal = r.get("signal", "")
        if signal == "BUY_YES": best_edge, best_ev = ye, yev
        elif signal == "BUY_NO": best_edge, best_ev = ne, nev
        else: best_edge, best_ev = None, None
        markets.append({
            "contract": get_contract_name(r),
            "market_date": r.get("market_date_local", ""),
            "countdown": get_settlement_countdown(r.get("market_date_local", ""), city),
            "p_yes": r.get("p_yes", ""),
            "yes_ask": r.get("yes_ask_price", ""),
            "no_ask": r.get("no_ask_price", ""),
            "best_edge": best_edge,
            "best_ev": best_ev,
            "signal": signal,
            "kelly": r.get("kelly_amount", ""),
            "predicted": r.get("predicted_daily_high", ""),
            "lead_day": r.get("lead_day", ""),
        })
    signals = {}
    for m in markets:
        sig = m["signal"] or "NO_TRADE"
        signals[sig] = signals.get(sig, 0) + 1
    dates = sorted(set(m["market_date"] for m in markets if m["market_date"]))
    predicted = markets[0]["predicted"] if markets else "?"
    lead = markets[0]["lead_day"] if markets else "?"
    countdown = markets[0]["countdown"] if markets else "?"
    model_info = ""
    model_path = proj / "data" / "models" / "empirical" / city / "empirical_model.json"
    if model_path.exists():
        md = json.loads(model_path.read_text("utf-8"))
        model_info = f"Empirical ECDF ({md.get('source_rows', '?')} samples, {md.get('train_start','')} \u2013 {md.get('train_end','')})"
    cities_data[city] = {
        "markets": markets, "signals": signals, "dates": dates,
        "predicted": predicted, "lead": lead, "countdown": countdown, "model_info": model_info
    }

generated_str = now_utc.strftime("%Y-%m-%d %H:%M UTC")
all_dates = sorted(set(d for c in cities_data.values() for d in c.get("dates", [])))
date_str = ", ".join(all_dates)

CSS = """<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#fff;color:#1a1a1a;padding:2rem;max-width:980px;margin:0 auto}
h1{font-size:20px;font-weight:700;letter-spacing:-.3px;margin-bottom:4px}
.subtitle{font-size:12px;color:#999;margin-bottom:2rem}
.city-section{margin-bottom:2.5rem}
.city-header{display:flex;align-items:baseline;gap:12px;margin-bottom:12px}
.city-name{font-size:16px;font-weight:700}
.city-meta{font-size:12px;color:#999}
.badge{font-size:11px;background:#f3f3f0;color:#666;padding:2px 8px;border-radius:5px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:14px}
.stat{background:#fafaf8;border:1px solid #ebebea;border-radius:8px;padding:10px 14px}
.stat-label{font-size:11px;color:#aaa;text-transform:uppercase;letter-spacing:.5px;margin-bottom:3px}
.stat-value{font-size:22px;font-weight:700;line-height:1}
.stat-value.yes{color:#1a5fb4}
.stat-value.no{color:#1c7a5e}
.stat-value.hold{color:#bbb}
table{width:100%;border-collapse:collapse;font-size:13px}
thead tr{border-bottom:1.5px solid #e8e8e4}
th{text-align:left;padding:7px 10px;font-weight:500;color:#aaa;font-size:11px;text-transform:uppercase;letter-spacing:.4px}
th.r{text-align:right}th.c{text-align:center}
td{padding:7px 10px;border-bottom:1px solid #f3f3f0}
td.r{text-align:right}td.c{text-align:center}
.pos{color:#1c7a5e;font-weight:600}
.neg{color:#c8c8c4}
.signal-tag{display:inline-block;padding:2px 9px;border-radius:5px;font-size:11px;font-weight:700;letter-spacing:.3px}
.tag-yes{background:#dbeafe;color:#1e40af}
.tag-no{background:#d1fae5;color:#065f46}
.tag-hold{background:#f3f3f0;color:#bbb}
.active-row td{color:#1a1a1a}
.inactive-row td{color:#c0bfba}
.divider{border-top:1px solid #ebebea;margin:2.5rem 0}
.footer{font-size:11px;color:#ccc;border-top:1px solid #ebebea;padding-top:12px;margin-top:.5rem;line-height:1.8}
</style>"""

def signal_tag(sig):
    if sig == "BUY_YES": return '<span class="signal-tag tag-yes">BUY YES</span>'
    elif sig == "BUY_NO": return '<span class="signal-tag tag-no">BUY NO</span>'
    else: return '<span class="signal-tag tag-hold">HOLD</span>'

def render_internal(city, data):
    if not data["markets"]:
        return f'<div class="city-section"><div class="city-name">{city}</div><p style="color:#ccc">No signals</p></div>'
    buy_yes = data["signals"].get("BUY_YES", 0)
    buy_no = data["signals"].get("BUY_NO", 0)
    no_trade = data["signals"].get("NO_TRADE", 0)
    h = f'''<div class="city-section">
<div class="city-header">
  <div class="city-name">{city}</div>
  <div class="city-meta">GFS forecast {data["predicted"]}&deg;C &bull; lead {data["lead"]}d &bull; {data["model_info"]}</div>
</div>
<div class="stats">
  <div class="stat"><div class="stat-label">Settlement</div><div class="stat-value" style="font-size:17px">{data["countdown"]}</div></div>
  <div class="stat"><div class="stat-label">Buy Yes</div><div class="stat-value yes">{buy_yes}</div></div>
  <div class="stat"><div class="stat-label">Buy No</div><div class="stat-value no">{buy_no}</div></div>
  <div class="stat"><div class="stat-label">No Trade</div><div class="stat-value hold">{no_trade}</div></div>
</div>
<table><thead><tr>
  <th>Contract</th>
  <th class="r">Model p(YES)</th>
  <th class="r">YES ask</th>
  <th class="r">NO ask</th>
  <th class="r">Best edge</th>
  <th class="r">EV</th>
  <th class="r">Kelly $</th>
  <th class="c">Signal</th>
</tr></thead><tbody>'''
    for m in data["markets"]:
        active = m["signal"] in ("BUY_YES", "BUY_NO")
        rc = "active-row" if active else "inactive-row"
        bold = ' style="font-weight:600"' if active else ""
        ec = "pos" if (m["best_edge"] or -1) > 0 else "neg"
        h += f'''<tr class="{rc}">
  <td{bold}>{m["contract"]}</td>
  <td class="r">{fmt_prob(m["p_yes"])}</td>
  <td class="r">{fmt_price(m["yes_ask"])}</td>
  <td class="r">{fmt_price(m["no_ask"])}</td>
  <td class="r {ec}">{fmt_pct(m["best_edge"])}</td>
  <td class="r {ec}">{fmt_dollar(m["best_ev"])}</td>
  <td class="r">{fmt_kelly(m["kelly"])}</td>
  <td class="c">{signal_tag(m["signal"])}</td>
</tr>'''
    h += "</tbody></table></div>"
    return h

def render_external(city, data):
    if not data["markets"]:
        return f'<div class="city-section"><div class="city-name">{city}</div><p style="color:#ccc">No signals</p></div>'
    h = f'''<div class="city-section">
<div class="city-header">
  <div class="city-name">{city}</div>
  <span class="badge">settles in {data["countdown"]}</span>
</div>
<table><thead><tr>
  <th>Contract</th>
  <th class="r">YES price</th>
  <th class="r">NO price</th>
  <th class="r">Best edge</th>
  <th class="r">EV</th>
  <th class="c">Signal</th>
</tr></thead><tbody>'''
    for m in data["markets"]:
        active = m["signal"] in ("BUY_YES", "BUY_NO")
        rc = "active-row" if active else "inactive-row"
        bold = ' style="font-weight:600"' if active else ""
        ec = "pos" if (m["best_edge"] or -1) > 0 else "neg"
        h += f'''<tr class="{rc}">
  <td{bold}>{m["contract"]}</td>
  <td class="r">{fmt_price(m["yes_ask"])}</td>
  <td class="r">{fmt_price(m["no_ask"])}</td>
  <td class="r {ec}">{fmt_pct(m["best_edge"])}</td>
  <td class="r {ec}">{fmt_dollar(m["best_ev"])}</td>
  <td class="c">{signal_tag(m["signal"])}</td>
</tr>'''
    h += "</tbody></table></div>"
    return h

# Build internal HTML
body_int = render_internal("London", cities_data["London"]) + '\n<div class="divider"></div>\n' + render_internal("Paris", cities_data["Paris"])
internal_html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>EV Signals \u2014 Internal</title>{CSS}</head>
<body>
<h1>Polymarket weather EV signals &mdash; Internal</h1>
<div class="subtitle">Settlement: {date_str} &nbsp;&bull;&nbsp; Prices: {generated_str} &nbsp;&bull;&nbsp; D1-only MVP &nbsp;&bull;&nbsp; Not for trading</div>
{body_int}
<div class="footer">Internal only &bull; Empirical ECDF model (D1-only MVP) &bull; Fee 2.5% unverified &bull; Min edge 3% &bull; CLOB prices {generated_str}</div>
</body></html>"""

# Build external HTML
body_ext = render_external("London", cities_data["London"]) + '\n<div class="divider"></div>\n' + render_external("Paris", cities_data["Paris"])
external_html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Weather Market Signals</title>{CSS}</head>
<body>
<h1>Weather market signals</h1>
<div class="subtitle">London &amp; Paris daily high temperature &nbsp;&bull;&nbsp; Settlement: {date_str} &nbsp;&bull;&nbsp; Prices as of {generated_str}</div>
{body_ext}
<div class="footer">Prices from Polymarket CLOB &bull; Signals by proprietary quantitative model &bull; Not financial advice</div>
</body></html>"""

(proj / "data" / "dashboard_internal.html").write_text(internal_html, encoding="utf-8")
(proj / "data" / "dashboard_external.html").write_text(external_html, encoding="utf-8")

print("=== 產出 ===")
print(f"内部版: data/dashboard_internal.html  ({len(internal_html):,} chars)")
print(f"外部版: data/dashboard_external.html  ({len(external_html):,} chars)")
for city in ["London", "Paris"]:
    d = cities_data[city]
    print(f"  {city}: {len(d['markets'])} markets | {d['signals']} | predicted={d['predicted']}C | countdown={d['countdown']}")
print()
print("Checklist:")
print(f"  [x] internal: {(proj / 'data' / 'dashboard_internal.html').exists()}")
print(f"  [x] external: {(proj / 'data' / 'dashboard_external.html').exists()}")
