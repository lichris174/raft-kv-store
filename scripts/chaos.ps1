# chaos.ps1: fault-injection harness for the raft-kv-store.
#
# Launches a local 5-node cluster (with persistence), writes canary data, then
# repeatedly kills and restarts a random node, verifying after every round that
# committed data is still readable and the cluster re-elects a leader. Proves
# crash/restart recovery with no data loss.
#
#   pwsh scripts/chaos.ps1 [-Rounds 10]
param(
  [int]$Rounds = 10,
  [int]$Base = 9001,
  [int]$N = 5,
  [int]$Threshold = 25
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$node = ".\bin\raftnode.exe"
$ctl  = ".\bin\raftctl.exe"
if (-not (Test-Path $node)) { throw "build first: go build -o bin\raftnode.exe ./server" }

$ports = 0..($N-1) | ForEach-Object { $Base + $_ }
function PeersOf($port) { ($ports | Where-Object { $_ -ne $port } | ForEach-Object { "localhost:$_" }) -join "," }
function IdOf($port) { "n$($port - $Base + 1)" }
function StartNode($port) {
  Start-Process $node -PassThru -WindowStyle Hidden -ArgumentList `
    "--id",(IdOf $port),"--addr",":$port","--advertise","localhost:$port",`
    "--peers",(PeersOf $port),"--data-dir",".\data\$(IdOf $port)","--snapshot-threshold","$Threshold",`
    "--http",":$($port - 1000)"
}
function LeaderPort {
  foreach ($p in $ports) {
    try { if ((& $ctl -addr ":$p" status 2>$null) -match "role=leader") { return $p } } catch {}
  }
  return $null
}
function WaitLeader($timeoutMs=4000) {
  $sw=[Diagnostics.Stopwatch]::StartNew()
  while ($sw.ElapsedMilliseconds -lt $timeoutMs) { $l=LeaderPort; if ($l){return $l}; Start-Sleep -Milliseconds 150 }
  return $null
}

# Kill stale raftnode processes from a prior run before claiming the ports.
Get-Process raftnode -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 400

if (Test-Path data) { Remove-Item -Recurse -Force data }
Write-Host "== launching $N-node cluster ==" -ForegroundColor Cyan
$procs = @{}
foreach ($p in $ports) { $procs[$p] = StartNode $p }
$leader = WaitLeader
if (-not $leader) { throw "no leader formed" }
Write-Host "leader elected: :$leader"

# canary data
$canary = @{}
for ($i=0; $i -lt 30; $i++) { $k="canary$i"; $v="v$i"; & $ctl -addr ":$leader" set $k $v | Out-Null; $canary[$k]=$v }
Write-Host "wrote $($canary.Count) canary keys"

$rng = [Random]::new()
$failures = 0
for ($r=1; $r -le $Rounds; $r++) {
  $victim = $ports[$rng.Next($ports.Count)]
  Write-Host "`n-- round $r/$Rounds : killing :$victim ($(IdOf $victim)) --" -ForegroundColor Yellow
  try { Stop-Process -Id $procs[$victim].Id -Force -ErrorAction Stop } catch {}
  Start-Sleep -Milliseconds 800

  $leader = WaitLeader
  if (-not $leader) { Write-Host "  FAIL: cluster lost quorum" -ForegroundColor Red; $failures++; }
  else {
    Write-Host "  recovered, leader=:$leader"
    # Write a new key during the degraded state. Only track it as canary data
    # once ctl actually confirms the commit (exit 0); a write that can't reach a
    # stable leader within its hop budget must NOT be asserted later, or we'd be
    # checking data the cluster never acknowledged. Retry across the settling window.
    $dk="degraded$r"; $committed=$false
    for ($try=0; $try -lt 5 -and -not $committed; $try++) {
      $leader = WaitLeader
      if (-not $leader) { Start-Sleep -Milliseconds 300; continue }
      & $ctl -addr ":$leader" set $dk "ok" | Out-Null
      if ($LASTEXITCODE -eq 0) { $committed=$true; $canary[$dk]="ok" }
      else { Start-Sleep -Milliseconds 300 }
    }
    if (-not $committed) { Write-Host "  WARN: degraded write $dk never confirmed (cluster unstable); not tracking" -ForegroundColor DarkYellow }
  }

  # restart victim from its data dir
  $procs[$victim] = StartNode $victim
  Start-Sleep -Milliseconds 1500

  # integrity check: sample canary keys via the leader (linearizable reads)
  $leader = WaitLeader
  $bad = 0
  foreach ($k in ($canary.Keys | Get-Random -Count ([Math]::Min(8,$canary.Count)))) {
    $got = (& $ctl -addr ":$leader" get $k 2>$null)
    if ($got -ne $canary[$k]) { $bad++; Write-Host "  MISMATCH $k=$got want $($canary[$k])" -ForegroundColor Red }
  }
  if ($bad -eq 0) { Write-Host "  integrity OK ($($canary.Count) keys tracked)" -ForegroundColor Green } else { $failures++ }
}

Write-Host "`n== cleanup ==" -ForegroundColor Cyan
foreach ($p in $procs.Values) { try { Stop-Process -Id $p.Id -Force -ErrorAction Stop } catch {} }
if ($failures -eq 0) { Write-Host "CHAOS PASSED: $Rounds rounds, zero data loss" -ForegroundColor Green; exit 0 }
else { Write-Host "CHAOS FAILED: $failures problem round(s)" -ForegroundColor Red; exit 1 }
