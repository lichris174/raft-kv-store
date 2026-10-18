# chaos.ps1: fault-injection harness for the raft-kv-store.
#
# Launches a local 5-node cluster with persistence and writes canary keys. Each
# round then kills a node (a random one, or the leader with -KillLeader), checks
# the survivors elect a leader and accept a write, restarts the victim, and reads
# back EVERY acknowledged key. At the end all nodes must hold identical data
# (the same state digest). Passing proves no acknowledged write was lost.
#
#   powershell -File scripts\chaos.ps1 [-Rounds 10] [-KillLeader] [-Seed N]
#
# The victims are chosen from -Seed (printed at the start), so a failing run can
# be replayed with the same sequence of kills.
#
# Output contract, parsed by dashboard/tests.js (keep the two in sync):
#   ROUND <r> KILL <nX> (<leader|random node>)
#   ROUND <r> RESULT=OK|FAIL <details>
#   CONVERGENCE RESULT=OK|FAIL <details>
#   CHAOS RESULT=PASS|FAIL <details>
param(
  [int]$Rounds = 10,
  [int]$Base = 9001,
  [int]$N = 5,
  [int]$Threshold = 25,
  [switch]$KillLeader,
  [int]$Seed = (Get-Random),
  # Outside OneDrive on purpose: sync clients lock files while nodes rewrite them.
  [string]$DataRoot = (Join-Path $env:TEMP "raft-kv-store\chaos")
)
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\lib.ps1"
Assert-Built raftnode
New-Cluster -Base $Base -N $N -DataRoot $DataRoot -Threshold $Threshold

$expected = [ordered]@{} # every acknowledged write: key -> value
$failures = 0

# Test-Expected reads back every acknowledged key from the leader and returns
# a description of each one that is missing or wrong.
function Test-Expected {
  $bad = New-Object System.Collections.Generic.List[string]
  foreach ($k in @($expected.Keys)) {
    $r = Get-Key $k 10
    if ($null -eq $r) { $bad.Add("no leader answered a read for $k"); break }
    if (-not $r.found) { $bad.Add("$k is missing") }
    elseif ($r.value -cne $expected[$k]) { $bad.Add("$k=$($r.value), want $($expected[$k])") }
  }
  , $bad
}

try {
  Reset-Cluster
  Write-Host "== launching $N-node cluster (data in $DataRoot, seed $Seed) ==" -ForegroundColor Cyan
  foreach ($p in $Cluster.Ports) { Start-Node $p }
  $leader = Wait-Leader
  if (-not $leader) { throw "no leader was elected" }
  Write-Host "leader elected: $(NodeId $leader)"
  for ($i = 0; $i -lt 30; $i++) {
    if (-not (Set-Key "canary$i" "v$i")) { throw "canary write $i was never acknowledged" }
    $expected["canary$i"] = "v$i"
  }
  Write-Host "wrote $($expected.Count) canary keys"

  $rng = [Random]::new($Seed)
  for ($r = 1; $r -le $Rounds; $r++) {
    $problems = New-Object System.Collections.Generic.List[string]
    $victim = $null
    if ($KillLeader) {
      $victim = Wait-Leader
      if (-not $victim) { $problems.Add("no leader to kill") }
    }
    $role = if ($victim) { "leader" } else { "random node" }
    if (-not $victim) { $victim = $Cluster.Ports[$rng.Next($N)] }
    Write-Host "`nROUND $r KILL $(NodeId $victim) ($role)" -ForegroundColor Yellow
    Stop-Node $victim

    # With one node down a majority remains, so the cluster must stay available.
    if (-not (Wait-Leader)) { $problems.Add("no leader elected with $($N - 1) of $N nodes up") }
    elseif (Set-Key "degraded$r" "ok") { $expected["degraded$r"] = "ok" }
    else { $problems.Add("a write failed with $($N - 1) of $N nodes up") }

    Start-Node $victim
    if (-not (Wait-Until { $null -ne (Get-NodeStatus $victim) } 15)) {
      $problems.Add("$(NodeId $victim) did not come back after restart (see $DataRoot\logs)")
    }
    foreach ($b in (Test-Expected)) { $problems.Add($b) }

    if ($problems.Count -gt 0) {
      $failures++
      Write-Host "ROUND $r RESULT=FAIL $($problems -join '; ')" -ForegroundColor Red
    } else {
      Write-Host "ROUND $r RESULT=OK all $($expected.Count) acknowledged keys read back" -ForegroundColor Green
    }
  }

  # Convergence: a final write commits in the leader's term, which carries every
  # earlier entry to every node. Then all nodes must hold byte-identical data.
  Write-Host "`n== convergence check ==" -ForegroundColor Cyan
  $finalWritten = Set-Key "final" "ok"
  if ($finalWritten) { $expected["final"] = "ok" }
  $deadline = (Get-Date).AddSeconds(20)
  do {
    $statuses = @($Cluster.Ports | ForEach-Object { Get-NodeStatus $_ -Digest } | Where-Object { $_ })
    $digests = @($statuses | ForEach-Object { $_.digest } | Select-Object -Unique)
    $converged = $statuses.Count -eq $N -and $digests.Count -eq 1
    if (-not $converged) { Start-Sleep -Milliseconds 300 }
  } while (-not $converged -and (Get-Date) -lt $deadline)
  $bad = Test-Expected
  if (-not $finalWritten) { $bad.Add("the final write was never acknowledged") }
  if ($converged -and $bad.Count -eq 0) {
    Write-Host "CONVERGENCE RESULT=OK all $N nodes hold identical data ($($statuses[0].keys) keys), every acknowledged key present" -ForegroundColor Green
  } else {
    $failures++
    $perNode = ($statuses | ForEach-Object { "$($_.nodeId): keys=$($_.keys) applied=$($_.lastApplied) digest=$($_.digest)" }) -join ", "
    Write-Host "CONVERGENCE RESULT=FAIL $($statuses.Count)/$N nodes answering; $perNode; $($bad -join '; ')" -ForegroundColor Red
  }
} catch {
  $failures++
  Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
} finally {
  Stop-Cluster
}

if ($failures -eq 0) {
  Write-Host "`nCHAOS RESULT=PASS $Rounds rounds, $($expected.Count) acknowledged writes, none lost" -ForegroundColor Green
  exit 0
}
Write-Host "`nCHAOS RESULT=FAIL $failures problem(s); node logs are in $DataRoot\logs" -ForegroundColor Red
exit 1
