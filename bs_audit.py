import json, urllib.request, re, time
B="http://192.168.207.67:5005"
def api(path):
    return json.load(urllib.request.urlopen(B+path,timeout=30))

STOP=set("the a an and or of to in on for with is are was were be been do does did this that it as at by from we you i your our their них a job task repo code fix build run file files use using make sure need needs should will can could would please reply exactly word single no commits change required".split())
CONFAB=("i don't have the ability","i do not have the ability","yes gemini is available","no code change required","no code change needed","here's what was built","i've now fetched","i have now fetched","as an ai","i cannot access","looks like a slack","i'm unable to")

def keywords(txt):
    return set(w for w in re.findall(r"[a-zA-Z_][a-zA-Z0-9_\.]{3,}", (txt or "").lower()) if w not in STOP)

def classify(j):
    k=(j.get("kind") or ""); r=j.get("result") or {}
    desc=j.get("description") or ""
    resp=str(r.get("summary") or r.get("full_response") or r.get("text") or r.get("output") or "")
    commits=r.get("commits") or []
    mand=(k in ("riskyeats","code") or k.startswith(("riskyeats:","feat:","fix:","argonaut:","riskybiz:","mnemos:","ic-engine","investorclaw","ncz-os")))
    cache=r.get("cache_hit")
    low=resp.lower()
    # signals
    if cache: return "ok-cache"
    if mand and not commits:
        if any(c in low for c in CONFAB): return "BS:confab+nocommit"
        # keyword overlap task vs response
        dk=keywords(desc); rk=keywords(resp)
        if dk:
            ov=len(dk & rk)/max(1,len(dk))
            if ov < 0.10: return "BS:offtask+nocommit"
        if len(resp.strip()) < 40: return "BS:empty+nocommit"
        return "BS:nocommit-mandatory"
    if mand and commits: return "ok-committed"
    if any(c in low for c in CONFAB): return "BS:confab"
    return "ok-noncommit"

jobs=api("/v1/jobs?status=done&limit=500").get("jobs") or []
now=time.time()
from collections import Counter
c=Counter(); bs=[]
for j in jobs:
    v=classify(j); c[v]+=1
    if v.startswith("BS:"): bs.append(j)
print("audited done jobs:",len(jobs))
for k,n in c.most_common(): print(f"  {n:4d}  {k}")
print("=== TOTAL BULLSHIT:",len(bs))
# breakdown by kind
print("  bullshit by kind:",dict(Counter((x.get('kind') or '?') for x in bs)))
# save bs ids+specs for refile
open("/tmp/_bs.json","w").write(json.dumps([{"id":x["id"],"kind":x.get("kind"),"description":x.get("description"),"priority":x.get("priority",0),"project":x.get("project")} for x in bs]))
print("  wrote",len(bs),"bullshit specs to /tmp/_bs.json")
