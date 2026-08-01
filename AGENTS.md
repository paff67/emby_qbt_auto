# AGENTS.md

## Cursor Cloud specific instructions

### What this is
`emby-qbt-auto` is a pure-Python 3.11+ orchestrator daemon (`qbt_orchestrator`) for qBittorrent scheduling, rclone uploads, gdrive-backfill scraping, and Emby refreshes. It is an outbound CLI/daemon — it does not listen on any port and has no GUI. State lives in a local SQLite file.

### Environment
- A virtualenv is created at `.venv` by the startup update script (`pip install -e ".[dev]"`). Use `.venv/bin/python` for all commands, or activate with `source .venv/bin/activate`.
- There are no third-party runtime dependencies (standard library only). The only dev dependency is `pytest`.

### Run the app (dry-run, no external services needed)
The daemon defaults to dry-run for nearly every subsystem, so it runs with only a writable SQLite path — no qBittorrent/rclone/Emby/Telegram/Docker required. Use `PYTHONPATH=src` (the `qbt_orchestrator/` top-level package is a thin wrapper around `src/qbt_orchestrator/`):

```bash
export PYTHONPATH=src
.venv/bin/python -m qbt_orchestrator.cli migrate --apply --state-db .tmp-state.sqlite   # create schema (default is dry-run without --apply)
.venv/bin/python -m qbt_orchestrator.cli once   --dry-run --state-db .tmp-state.sqlite
.venv/bin/python -m qbt_orchestrator.cli status --json --state-db .tmp-state.sqlite
# Bounded daemon loop (otherwise runs forever): --max-safety-ticks caps ticks, --safety-interval 0 avoids sleeps
.venv/bin/python -m qbt_orchestrator.cli daemon --dry-run --state-db .tmp-state.sqlite --max-safety-ticks 2 --safety-interval 0
```
Non-obvious: `migrate` is dry-run unless you pass `--apply`; the CLI also auto-migrates when the state DB file does not yet exist. `*.sqlite` files are gitignored, so temp state DBs won't be committed.

### Test
No external services are required — all integrations (qBT/rclone/Emby/Telegram/filesystem/backfill) are faked under `tests/`.
```bash
.venv/bin/python -m pytest -q
```

### Lint
No linter is configured in this repo (no ruff/flake8/mypy/black config). There is no lint step to run.

### Deployment assets (not for local dev)
`deploy/systemd/` and `deploy/scripts/` are for production VPS deployment (systemd unit + install/rollback/backup shell scripts). They are not needed to develop or test locally.
