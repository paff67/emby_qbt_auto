param(
  [string]$Target = "paff-vps",
  [string]$OutDir = "legacy/live_$(Get-Date -Format yyyyMMdd-HHmmss)"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$dest = Join-Path $repo $OutDir
New-Item -ItemType Directory -Force $dest | Out-Null

$files = @(
  "orchestrator.py",
  "qbt_add_checked.py",
  "qbt_add_webui.py",
  "junk_rules.py"
)
foreach ($f in $files) {
  scp -q "$Target`:/opt/qbt-orchestrator/$f" (Join-Path $dest $f)
}
scp -q -r "$Target`:/opt/qbt-orchestrator/tests" (Join-Path $dest "tests")

Write-Host "Fetched non-sensitive live code snapshot to $dest"
Write-Host "Do not fetch /etc/qbt-orchestrator, /var/lib/qbt-orchestrator, rclone.conf, logs, cookies, or tokens into git."
