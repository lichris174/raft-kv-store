# bench.ps1: launch a local cluster, run the benchmark, save a report.
#
#   powershell -File scripts\bench.ps1 [-N 5] [-Writers 24] [-Readers 16] [-Duration 5s] [-Fsync]
#
# -Fsync starts the cluster with --fsync so every durable write is flushed to
# disk before it is acknowledged. Use it to measure the power-loss-durability
# latency tradeoff against the default (page-cache) mode. The exit code is
# raftbench's: non-zero if the cluster never came up or any operation failed.
param(
  [int]$N = 5,
  [int]$Writers = 24,
  [int]$Readers = 16,
  [string]$Duration = "5s",
  [int]$Base = 9001,
  [int]$Threshold = 2000,
  [switch]$Fsync,
  # Outside OneDrive on purpose: sync clients lock files while nodes rewrite them.
  [string]$DataRoot = (Join-Path $env:TEMP "raft-kv-store\bench")
)
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\lib.ps1"
Assert-Built raftnode, raftbench
$nodeArgs = @("--read-mode", "follower")
if ($Fsync) { $nodeArgs += "--fsync" }
New-Cluster -Base $Base -N $N -DataRoot $DataRoot -Threshold $Threshold -NodeArgs $nodeArgs

$code = 1
try {
  Reset-Cluster
  Write-Host "launching $N-node cluster..." -ForegroundColor Cyan
  foreach ($p in $Cluster.Ports) { Start-Node $p }
  # Nodes answer status as followers during the election, so wait for a leader
  # too; otherwise the benchmark would start before anyone can accept writes.
  $ready = Wait-Until { @($Cluster.Ports | Where-Object { Get-NodeStatus $_ }).Count -eq $N -and (Get-LeaderPort) } 15
  if (-not $ready) { throw "cluster not ready after 15s (node logs: $DataRoot\logs)" }
  Write-Host "all $N nodes up, leader elected" -ForegroundColor Green

  $addrs = ($Cluster.Ports | ForEach-Object { "localhost:$_" }) -join ","
  $ErrorActionPreference = "Continue" # a native program's stderr must not abort the script
  $report = & "$Root\bin\raftbench.exe" -addrs $addrs -writers $Writers -readers $Readers -duration $Duration -keyspace 20000 2>&1 |
    ForEach-Object { "$_" }
  $code = $LASTEXITCODE
  $ErrorActionPreference = "Stop"
  $report | ForEach-Object { Write-Host $_ }
  try {
    $report + "" + "generated: $(Get-Date -Format o)  nodes=$N writers=$Writers readers=$Readers duration=$Duration fsync=$Fsync" |
      Out-File -Encoding utf8 "$Root\benchmark\report.txt"
    Write-Host "`nreport saved to benchmark\report.txt" -ForegroundColor Green
  } catch {
    Write-Host "WARN: could not save the report: $($_.Exception.Message)" -ForegroundColor Yellow
  }
} catch {
  Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
} finally {
  Stop-Cluster
}
exit $code
