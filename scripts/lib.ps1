# lib.ps1: helpers shared by chaos.ps1 and bench.ps1. Dot-source it:
#
#   . "$PSScriptRoot\lib.ps1"
#
# Nodes are driven through their HTTP gateway with Invoke-RestMethod rather than
# by launching raftctl.exe for every call. That is much faster, and a failed
# request is an exception the caller handles. (With $ErrorActionPreference =
# "Stop", Windows PowerShell turns any stderr text from a native command into a
# terminating error, which used to kill a whole chaos run over one refused read.)

$script:Root = Split-Path -Parent $PSScriptRoot
$ProgressPreference = "SilentlyContinue" # progress rendering slows every Invoke-RestMethod
$script:HttpOffset = 1000 # a node's HTTP port is its gRPC port minus this

$script:Packages = @{ raftnode = "./server"; raftbench = "./benchmark"; raftctl = "./client" }

function Assert-Built([string[]]$Names) {
  foreach ($n in $Names) {
    if (-not (Test-Path "$Root\bin\$n.exe")) { throw "build first: go build -o bin\$n.exe $($Packages[$n])" }
  }
}

# New-Cluster describes the cluster the other functions act on. Ports run from
# Base to Base+N-1; NodeArgs are extra raftnode flags for every node.
function New-Cluster([int]$Base, [int]$N, [string]$DataRoot, [int]$Threshold, [string[]]$NodeArgs = @()) {
  $script:Cluster = [pscustomobject]@{
    Base = $Base; N = $N; Ports = @(0..($N - 1) | ForEach-Object { $Base + $_ })
    DataRoot = $DataRoot; Threshold = $Threshold; NodeArgs = $NodeArgs; Procs = @{}
  }
}

function NodeId([int]$Port) { "n$($Port - $Cluster.Base + 1)" }

# Reset-Cluster kills raftnode processes left by an earlier harness or demo run
# (their data lives under %TEMP%\raft-kv-store; no other raftnode is touched),
# makes sure nothing else holds the cluster's ports, and wipes the data directory.
function Reset-Cluster {
  Get-CimInstance Win32_Process -Filter "Name='raftnode.exe'" |
    Where-Object { $_.CommandLine -like "*raft-kv-store*" } |
    ForEach-Object {
      try { $p = Get-Process -Id $_.ProcessId -ErrorAction Stop; $p.Kill(); [void]$p.WaitForExit(10000) } catch {}
    }
  $ports = @($Cluster.Ports) + @($Cluster.Ports | ForEach-Object { $_ - $HttpOffset })
  foreach ($c in @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $ports -contains $_.LocalPort })) {
    $owner = Get-Process -Id $c.OwningProcess -ErrorAction SilentlyContinue
    throw "port $($c.LocalPort) is already in use by $($owner.ProcessName) (pid $($c.OwningProcess)); stop it first"
  }
  for ($i = 0; Test-Path $Cluster.DataRoot; $i++) {
    try { Remove-Item -Recurse -Force $Cluster.DataRoot -ErrorAction Stop }
    catch {
      if ($i -ge 20) { throw "could not delete $($Cluster.DataRoot): $_" }
      Start-Sleep -Milliseconds 200 # a just-killed process can hold files briefly
    }
  }
  New-Item -ItemType Directory -Force (Join-Path $Cluster.DataRoot "logs") | Out-Null
}

# Start-Node launches one node, resuming from its data directory. Each start
# gets its own log file under <DataRoot>\logs, so a node that dies leaves a trace.
function Start-Node([int]$Port) {
  $id = NodeId $Port
  $peers = ($Cluster.Ports | Where-Object { $_ -ne $Port } | ForEach-Object { "localhost:$_" }) -join ","
  $nodeArgs = @(
    "--id", $id, "--addr", ":$Port", "--advertise", "localhost:$Port", "--peers", $peers,
    "--data-dir", "`"$(Join-Path $Cluster.DataRoot $id)`"", "--snapshot-threshold", "$($Cluster.Threshold)",
    "--http", ":$($Port - $HttpOffset)"
  ) + $Cluster.NodeArgs
  $log = Join-Path $Cluster.DataRoot "logs\$id-$(Get-Date -Format 'HHmmss-fff')"
  $Cluster.Procs[$Port] = Start-Process "$Root\bin\raftnode.exe" -PassThru -WindowStyle Hidden `
    -ArgumentList $nodeArgs -RedirectStandardOutput "$log.out" -RedirectStandardError "$log.log"
}

# Stop-Node kills a node the hard way (like a crash) and waits until it is gone.
function Stop-Node([int]$Port) {
  $p = $Cluster.Procs[$Port]
  if ($p) {
    try { $p.Kill(); [void]$p.WaitForExit(10000) } catch {}
    $Cluster.Procs.Remove($Port)
  }
}

function Stop-Cluster { foreach ($port in @($Cluster.Procs.Keys)) { Stop-Node $port } }

# 127.0.0.1 rather than localhost: on Windows a request to a closed port via
# "localhost" tries ::1 and then 127.0.0.1, with SYN retries, about 4s in all.
function Invoke-Node([int]$Port, [string]$Method, [string]$PathAndQuery) {
  Invoke-RestMethod -Method $Method -Uri "http://127.0.0.1:$($Port - $HttpOffset)$PathAndQuery" -TimeoutSec 5
}

# Get-NodeStatus returns a node's /status (with -Digest, including the state
# digest), or $null if it does not answer.
function Get-NodeStatus([int]$Port, [switch]$Digest) {
  $q = if ($Digest) { "?digest=1" } else { "" }
  try { Invoke-Node $Port GET "/status$q" } catch { $null }
}

# Get-LeaderPort returns the port of the leader with the highest term, or $null.
# Only running nodes are asked: a request to a killed one costs seconds.
function Get-LeaderPort {
  $best = $null; $bestTerm = -1
  foreach ($p in @($Cluster.Procs.Keys)) {
    $s = Get-NodeStatus $p
    if ($s -and $s.role -eq "leader" -and $s.term -gt $bestTerm) { $best = $p; $bestTerm = $s.term }
  }
  $best
}

# Wait-Until polls a condition every 200ms until it holds or TimeoutSec passes.
function Wait-Until([scriptblock]$Condition, [int]$TimeoutSec) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ($true) {
    if (& $Condition) { return $true }
    if ((Get-Date) -gt $deadline) { return $false }
    Start-Sleep -Milliseconds 200
  }
}

function Wait-Leader([int]$TimeoutSec = 15) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  do {
    $l = Get-LeaderPort
    if ($l) { return $l }
    Start-Sleep -Milliseconds 200
  } while ((Get-Date) -lt $deadline)
  $null
}

function Esc([string]$s) { [uri]::EscapeDataString($s) }

# Set-Key writes through the current leader, retrying through elections, and
# returns $true once the write is acknowledged (committed by a majority).
function Set-Key([string]$Key, [string]$Value, [int]$TimeoutSec = 15) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  do {
    $l = Get-LeaderPort
    if ($l) {
      try { if ((Invoke-Node $l POST "/set?key=$(Esc $Key)&value=$(Esc $Value)").ok) { return $true } } catch {}
    }
    Start-Sleep -Milliseconds 200
  } while ((Get-Date) -lt $deadline)
  $false
}

# Get-Key reads a key from the leader (a linearizable read when the cluster runs
# in leader read mode). Returns the reply ({found, value}) or $null if no leader
# could answer in time.
function Get-Key([string]$Key, [int]$TimeoutSec = 15) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  do {
    $l = Get-LeaderPort
    if ($l) {
      try {
        $r = Invoke-Node $l GET "/get?key=$(Esc $Key)"
        if (-not $r.leaderHint) { return $r } # a hint means leadership moved; ask again
      } catch {}
    }
    Start-Sleep -Milliseconds 200
  } while ((Get-Date) -lt $deadline)
  $null
}
