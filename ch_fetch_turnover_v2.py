#!/usr/bin/env python3
"""
ch_fetch_turnover_v2.py — 4PS Prospecting Intelligence Model, Step 1 (turnover enrichment), revised

WHAT IT DOES
  For each company number in the request list, fetches the latest electronically filed accounts from the
  free Companies House API, extracts Turnover/Revenue (current + prior year) and average employees from the
  iXBRL, and appends to a results CSV. Resumable: re-running skips companies already in the results file.

WHAT CHANGED vs v1 (after the first Colab attempt failed)
  --test            checks your API key with one call and explains any 401/403/429 before starting
  --limit N         process only the next N companies (run in chunks of e.g. 300 so a disconnect costs little)
  --rank R          process only rows with Priority_Rank <= R (1 = most important) so partial runs are useful
  clearer errors    every failure row carries an HTTP status and reason
  Retry-After       honours the header on 429 instead of guessing
  progress file     prints a one-line summary at the end (ok / no turnover tag / PDF-only / errors)

USAGE (terminal or Colab cell; requests + beautifulsoup4 + lxml required)
  export CH_API_KEY=your_key            (Windows PowerShell:  $env:CH_API_KEY="your_key")
  python3 ch_fetch_turnover_v2.py --test
  python3 ch_fetch_turnover_v2.py CH_turnover_request_list_v2.csv CH_turnover_results.csv --rank 2 --limit 300
  ... repeat the last line until it reports "nothing left to do"

KEY NOTES
  - Key must be a REST API key for the LIVE environment (not Streaming, not Sandbox). Create at
    https://developer.company-information.service.gov.uk/manage-applications
  - 401 = key wrong/missing; 403 = wrong key type or IP restriction set on the application; 429 = rate limit.
"""
import csv, os, sys, time, re, argparse
try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("Missing packages. Run:  pip install requests beautifulsoup4 lxml")

API = "https://api.company-information.service.gov.uk"
KEY = os.environ.get("CH_API_KEY", "").strip()

def session():
    if not KEY: sys.exit("CH_API_KEY is not set. See usage in the file header.")
    s = requests.Session(); s.auth = (KEY, ""); s.headers["User-Agent"] = "4PS-prospecting-model/1.0"
    return s

def get(S, url, **kw):
    for attempt in range(8):
        try:
            r = S.get(url, timeout=60, **kw)
        except requests.RequestException as e:
            time.sleep(3 + attempt * 3); continue
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "60")); print(f"  rate limited, waiting {wait}s", flush=True); time.sleep(wait); continue
        if r.status_code in (401, 403):
            return r
        if r.status_code in (200, 404): return r
        time.sleep(2 + attempt * 3)
    return r

def explain(r):
    return {401: "401 Unauthorised — key missing or wrong (copy the key exactly; use REST key, LIVE environment)",
            403: "403 Forbidden — wrong key type (Streaming?) or IP restriction on the application, or Sandbox key against LIVE",
            404: "404 — company not found", 429: "429 — rate limited"}.get(r.status_code, f"{r.status_code} {r.reason}")

def test_key():
    S = session(); r = get(S, f"{API}/company/00000006")   # a long-standing public company number
    if r.status_code == 200:
        print("API key OK — connected as REST/live. Company 00000006 =", r.json().get("company_name")); return True
    print("API key test FAILED:", explain(r)); return False

TURNOVER_TAGS = ("turnover", "revenue", "turnoverrevenue")
EMP_TAGS = ("averagenumberemployeesduringperiod", "averagenumberofemployees", "employees")

def to_num(cell):
    txt = cell.get_text(strip=True).replace(",", "").replace("(", "-").replace(")", "")
    txt = re.sub(r"[^\d.\-]", "", txt)
    if not txt or txt in ("-", "."): return None
    try: v = float(txt)
    except ValueError: return None
    try: v *= 10 ** int(cell.get("scale", "0") or 0)
    except ValueError: pass
    if cell.get("sign") == "-": v = -v
    return v

def parse_ixbrl(html):
    soup = BeautifulSoup(html, "lxml")
    ctx = {}
    for c in soup.find_all(re.compile(r"(xbrli:)?context$")):
        cid = c.get("id"); end = c.find(re.compile(r"(xbrli:)?(enddate|instant)$"))
        if cid and end: ctx[cid] = end.get_text(strip=True)
    facts = []
    for f in soup.find_all(re.compile(r"ix:nonfraction", re.I)):
        name = (f.get("name") or "").lower(); local = name.split(":")[-1]; v = to_num(f)
        if v is not None: facts.append((local, name, ctx.get(f.get("contextref"), ""), v, f.get("unitref", "")))
    def pick(tagset):
        cands = [x for x in facts if x[0] in tagset] or [x for x in facts if any(t in x[0] for t in tagset)]
        cands = [x for x in cands if x[3] >= 0]
        if not cands: return None, None, None, []
        ends = sorted(set(x[2] for x in cands), reverse=True)
        cur = [x for x in cands if x[2] == ends[0]]; prior = [x for x in cands if len(ends) > 1 and x[2] == ends[1]]
        return (max(cur, key=lambda x: x[3])[3] if cur else None, max(prior, key=lambda x: x[3])[3] if prior else None,
                ends[0], sorted(set(x[1] for x in cands)))
    t, tp, pe, tags = pick(TURNOVER_TAGS); e, _, _, etags = pick(EMP_TAGS)
    units = set(x[4] for x in facts if x[0] in TURNOVER_TAGS)
    return dict(Turnover_GBP=t, Turnover_PriorYear_GBP=tp, PeriodEnd=pe, AvgEmployees=e, Currency=",".join(sorted(units)), Source_Tags=";".join(tags + etags)[:300])

def process(S, num):
    fh = get(S, f"{API}/company/{num}/filing-history", params={"category": "accounts", "items_per_page": 10})
    if fh.status_code != 200: return dict(Status="error", Note=explain(fh))
    items = [i for i in fh.json().get("items", []) if i.get("category") == "accounts" and i.get("links", {}).get("document_metadata")]
    if not items: return dict(Status="no accounts filing", Note="")
    items.sort(key=lambda i: i.get("date", ""), reverse=True)
    for it in items[:2]:
        meta = get(S, it["links"]["document_metadata"])
        if meta.status_code != 200: continue
        if "application/xhtml+xml" not in meta.json().get("resources", {}): continue
        doc = get(S, f"{it['links']['document_metadata']}/content", headers={"Accept": "application/xhtml+xml"}, allow_redirects=True)
        if doc.status_code != 200: continue
        out = parse_ixbrl(doc.text); out["AccountsType"] = it.get("description", ""); out["FilingDate"] = it.get("date", "")
        out["Status"] = "ok" if out.get("Turnover_GBP") else "ixbrl without turnover tag (small/filleted or group-only)"
        return out
    return dict(Status="no ixbrl (PDF only)", Note="paper filing — use sibling entity or paid source")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src", nargs="?"); ap.add_argument("dst", nargs="?")
    ap.add_argument("--test", action="store_true"); ap.add_argument("--limit", type=int, default=0); ap.add_argument("--rank", type=int, default=99)
    a = ap.parse_args()
    if a.test or not a.src: sys.exit(0 if test_key() else 1)
    if not test_key(): sys.exit(1)
    S = session()
    done = set()
    if os.path.exists(a.dst): done = {r["CH_Number"] for r in csv.DictReader(open(a.dst, newline=""))}
    rows = [r for r in csv.DictReader(open(a.src, newline="")) if int(r.get("Priority_Rank", 1)) <= a.rank and r["CH_Number"] not in done]
    if not rows: print("nothing left to do for this rank"); return
    if a.limit: rows = rows[:a.limit]
    fields = ["CH_Number", "CH_Name", "Group_ID", "Group_Name", "Priority_Tier", "Priority_Rank", "PeriodEnd", "FilingDate", "Turnover_GBP",
              "Turnover_PriorYear_GBP", "AvgEmployees", "AccountsType", "Currency", "Source_Tags", "Status", "Note"]
    new = not os.path.exists(a.dst)
    tally = {}
    with open(a.dst, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new: w.writeheader()
        for i, r in enumerate(rows, 1):
            res = {k: r.get(k, "") for k in ("CH_Number", "CH_Name", "Group_ID", "Group_Name", "Priority_Tier", "Priority_Rank")}
            try: res.update(process(S, r["CH_Number"]))
            except Exception as e: res.update(Status="exception", Note=str(e)[:150])
            w.writerow(res); f.flush(); tally[res["Status"]] = tally.get(res["Status"], 0) + 1
            if i % 25 == 0: print(f"{i}/{len(rows)}  {tally}", flush=True)
            time.sleep(1.6)
    print("chunk finished ->", a.dst, tally)

if __name__ == "__main__":
    main()
