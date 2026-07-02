# Status: completed on 2026-07-02 via root SSH

See docs/prep/root-verification-20260702.md for the redacted results. The commands below remain for reproducibility.

# Root-only read-only data still needed

Codex verified that `sudo -n true` fails for `paffops`, so this data was intentionally not collected.
Run these in an interactive SSH terminal if we need to complete the pre-deploy baseline.

```bash
ssh paff-vps

# Redacted config shape, not raw secrets
sudo python3 - <<'PY'
import json, re
paths=["/etc/qbt-orchestrator/config.json"]
SECRET=re.compile(r"(token|password|passwd|secret|key|apikey|api_key|cookie|authorization|auth|credential)", re.I)
def red(x,k=""):
    if isinstance(x, dict): return {kk:("<redacted>" if SECRET.search(kk) else red(v,kk)) for kk,v in x.items()}
    if isinstance(x, list): return [red(v,k) for v in x]
    if isinstance(x, str) and SECRET.search(k): return "<redacted>"
    return x
for p in paths:
    print("---", p)
    print(json.dumps(red(json.load(open(p))), ensure_ascii=False, indent=2))
PY

# Current DB schema/counts only
sudo sqlite3 /var/lib/qbt-orchestrator/state.sqlite '.tables'
sudo sqlite3 /var/lib/qbt-orchestrator/state.sqlite "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name;"
for t in $(sudo sqlite3 /var/lib/qbt-orchestrator/state.sqlite "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"); do
  printf '%s=' "$t"; sudo sqlite3 /var/lib/qbt-orchestrator/state.sqlite "SELECT count(*) FROM \"$t\";"
done

# gdrive-backfill status, no env values containing secrets
sudo sed -E 's/^([^#=]*(TOKEN|PASSWORD|PASS|SECRET|KEY|COOKIE|AUTH|CREDENTIAL)[^=]*)=.*/\1=<redacted>/Ig' /opt/qbt/gdrive-backfill/config/backfill.env
sudo cat /opt/qbt/gdrive-backfill/config/roots.txt
sudo cat /opt/qbt/gdrive-backfill/logs/latest.summary
sudo tail -n 120 /opt/qbt/gdrive-backfill/logs/latest.log

# root rclone presence/capacity only, do not print rclone.conf
sudo rclone listremotes
sudo rclone about gcrypt: --json
```

Do not paste raw tokens, rclone config, cookies, API keys, or full magnet links into chat.

