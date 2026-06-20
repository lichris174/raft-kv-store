# bench.ps1: launch a local cluster, run the benchmark, save a report.
#
#   pwsh scripts/bench.ps1 [-N 5] [-Writers 24] [-Readers 16] [-Duration 5s]
param(
  [int]$N = 5,
  [int]$Writers = 24,
  [int]$Readers = 16,
  [string]$Duration = "5s",
  [int]$Base = 9001,
  [int]$Threshold = 2000
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if (-not (Test-Path .\bin\raftnode.exe)) { throw "build first: go build -o bin\raftnode.exe ./server" }
if (-not (Test-Path .\bin\raftctl.exe))  { throw "build first: go build -o bin\raftctl.exe ./client" }
if (-not (Test-Path .\bin\raftbench.exe)) { throw "build first: go build -o bin\raftbench.exe ./benchmark" }

$ports = 0..($N-1) | ForEach-Object { $Base + $_ }
function Node($port) {
  $id = "n$($port - $Base + 1)"
  $peers = ($ports | Where-Object { $_ -ne $port } | ForEach-Object { "localhost:$_" }) -join ","
  Start-Process .\bin\raftnode.exe -PassThru -WindowStyle Hidden -ArgumentList `
    "--id",$id,"--addr",":$port","--advertise","localhost:$port","--peers",$peers,`
    "--read-mode","follower","--data-dir",".\databench\$id","--snapshot-threshold","$Threshold",`
    "--http",":$($port - 1000)"
}

# Kill stale raftnode processes from a prior chaos/bench run (they share ports
# 9001-9005). A straggler holding a port makes a new node fail to bind, which then
# eats ~1/N of round-robin reads as timeouts and inflates the error count.
Get-Process raftnode -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 400

if (Test-Path databench) { Remove-Item -Recurse -Force databench }
Write-Host "launching $N-node cluster..." -ForegroundColor Cyan
$procs = $ports | ForEach-Object { Node $_ }

# Wait until every node answers status AND a leader has been elected before
# measuring. Nodes answer status as followers during the election, so checking
# reachability alone would start the benchmark before a leader exists (then
# resolveLeader finds none). A node that never comes up can no longer skew results.
$ctl = ".\bin\raftctl.exe"
$sw = [Diagnostics.Stopwatch]::StartNew()
$up = 0; $haveLeader = $false
do {
  $statuses = $ports | ForEach-Object { try { & $ctl -addr ":$_" status 2>$null } catch { "" } }
  $up = @($statuses | Where-Object { $_ -match "role=" }).Count
  $haveLeader = @($statuses | Where-Object { $_ -match "role=leader" }).Count -gt 0
  if ($up -eq $N -and $haveLeader) { break }
  Start-Sleep -Milliseconds 200
} while ($sw.ElapsedMilliseconds -lt 8000)
if ($up -ne $N -or -not $haveLeader) {
  $procs | ForEach-Object { try { Stop-Process -Id $_.Id -Force -ErrorAction Stop } catch {} }
  throw "cluster not ready: $up/$N nodes up, leader=$haveLeader after 8s"
}
Write-Host "all $N nodes up, leader elected" -ForegroundColor Green

$addrs = ($ports | ForEach-Object { "localhost:$_" }) -join ","
$report = .\bin\raftbench.exe -addrs $addrs -writers $Writers -readers $Readers -duration $Duration -keyspace 20000
$report | Tee-Object -FilePath .\benchmark\report.txt
Add-Content .\benchmark\report.txt "`ngenerated: $(Get-Date -Format o)  nodes=$N writers=$Writers readers=$Readers duration=$Duration"

$procs | ForEach-Object { try { Stop-Process -Id $_.Id -Force -ErrorAction Stop } catch {} }
Write-Host "`nreport saved to benchmark/report.txt" -ForegroundColor Green
