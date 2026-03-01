# Run benchmarks and update README with results.
# Usage:
#   .\scripts\bench_to_readme.ps1                     # run all libs, merge, update README
#   .\scripts\bench_to_readme.ps1 -Lib default         # run only default (kcp_tokio, quinn, kcp_deepseek, kcprs) — no optional features
#   .\scripts\bench_to_readme.ps1 -Lib ys_kcp          # run only ys_kcp, merge into existing, update README
#   .\scripts\bench_to_readme.ps1 -Lib kcp_sys         # run only kcp_sys, merge into existing, update README
#   .\scripts\bench_to_readme.ps1 -Lib slipstream_picoquic  # run only slipstream-picoquic (requires C lib built)
#   .\scripts\bench_to_readme.ps1 -Lib quinn                # run only quinn (QUIC) benchmarks
#   .\scripts\bench_to_readme.ps1 -Lib kcprs                # run only kcprs benchmarks
#   .\scripts\bench_to_readme.ps1 -SkipRun             # only update README from existing target\criterion

param(
    [switch]$SkipRun,
    [ValidateSet('all', 'default', 'ys_kcp', 'kcp_sys', 'slipstream_picoquic', 'quinn', 'kcprs')]
    [string]$Lib = 'all'
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $ProjectRoot "Cargo.toml"))) {
    $ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
}

$CriterionDir = Join-Path $ProjectRoot "target\criterion"
$CriterionMerged = Join-Path $ProjectRoot "target\criterion_merged"
$SuccessLogPath = Join-Path $ProjectRoot "target\criterion_bench_success.log"
$ReadmePath = Join-Path $ProjectRoot "README.md"

function Merge-GroupIntoMerged($groupName) {
    $src = Join-Path $CriterionDir $groupName
    $dst = Join-Path $CriterionMerged $groupName
    if (Test-Path $src) {
        if (Test-Path $dst) { Remove-Item -Recurse -Force $dst }
        Copy-Item -Recurse -Force $src $dst
    }
}

if (-not $SkipRun) {
    Push-Location $ProjectRoot
    try {
        if (Test-Path $SuccessLogPath) { Remove-Item -Force $SuccessLogPath }
        if ($Lib -eq 'all') {
            # Full run: nightly+ys-kcp, then kcp-sys if libclang
            Write-Host "Ensuring nightly toolchain is installed (required for ys_kcp)..."
            & rustup toolchain install nightly 2>&1 | Out-Host
            Write-Host "Running benchmarks (nightly + ys-kcp)..."
            & rustup run nightly cargo bench --features ys-kcp 2>&1 | Out-Host
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "Nightly + ys-kcp bench failed; running default bench only."
                Remove-Item -Recurse -Force $CriterionDir -ErrorAction SilentlyContinue
                & cargo bench 2>&1 | Out-Host
                if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            }
            if (Test-Path $CriterionMerged) { Remove-Item -Recurse -Force $CriterionMerged }
            Copy-Item -Recurse -Force $CriterionDir $CriterionMerged
            foreach ($g in @("kcp_sys_throughput", "kcp_sys_latency")) {
                $d = Join-Path $CriterionMerged $g
                if (Test-Path $d) { Remove-Item -Recurse -Force $d }
            }
            $libclangAvailable = $false
            if ($env:LIBCLANG_PATH) {
                $libclangAvailable = Test-Path (Join-Path $env:LIBCLANG_PATH "libclang.dll") -ErrorAction SilentlyContinue
                if (-not $libclangAvailable) { $libclangAvailable = Test-Path (Join-Path $env:LIBCLANG_PATH "clang.dll") -ErrorAction SilentlyContinue }
            }
            if (-not $libclangAvailable -and $env:Path) {
                foreach ($dir in $env:Path -split ';') {
                    if (Test-Path (Join-Path $dir "libclang.dll") -ErrorAction SilentlyContinue) { $libclangAvailable = $true; break }
                    if (Test-Path (Join-Path $dir "clang.dll") -ErrorAction SilentlyContinue) { $libclangAvailable = $true; break }
                }
            }
            if ($libclangAvailable) {
                Write-Host "Running benchmarks (kcp-sys)..."
                & cargo bench --features kcp-sys 2>&1 | Out-Host
                if ($LASTEXITCODE -eq 0) {
                    Merge-GroupIntoMerged "kcp_sys_throughput"
                    Merge-GroupIntoMerged "kcp_sys_latency"
                }
            } else {
                Write-Host "Skipping kcp-sys (libclang not found)."
            }
            # Optional: slipstream-picoquic (requires C lib built via scripts/build_slipstream_picoquic.sh)
            Write-Host "Trying benchmarks (slipstream-picoquic)..."
            $slipOk = $false
            try {
                & cargo bench --features slipstream-picoquic 2>&1 | Out-Host
                if ($LASTEXITCODE -eq 0) {
                    foreach ($g in @("slipstream_throughput", "slipstream_latency", "slipstream_concurrent")) {
                        Merge-GroupIntoMerged $g
                    }
                    $slipOk = $true
                }
            } catch {}
            if (-not $slipOk) {
                Write-Host "Skipping slipstream-picoquic (build or run failed; run scripts/build_slipstream_picoquic.sh to enable)."
            }
            Remove-Item -Recurse -Force $CriterionDir
            Rename-Item -Path $CriterionMerged -NewName "criterion"
        } elseif ($Lib -eq 'default') {
            Write-Host "Running benchmarks (default: kcp_tokio, quinn, kcp_deepseek, kcprs)..."
            & cargo bench 2>&1 | Out-Host
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        } elseif ($Lib -eq 'ys_kcp') {
            if (Test-Path $CriterionDir) {
                if (Test-Path $CriterionMerged) { Remove-Item -Recurse -Force $CriterionMerged }
                Copy-Item -Recurse -Force $CriterionDir $CriterionMerged
            }
            Write-Host "Ensuring nightly toolchain is installed..."
            & rustup toolchain install nightly 2>&1 | Out-Host
            Write-Host "Running benchmarks (nightly + ys-kcp)..."
            & rustup run nightly cargo bench --features ys-kcp -- ys_kcp 2>&1 | Out-Host
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            if (Test-Path $CriterionMerged) {
                foreach ($g in @("ys_kcp_throughput", "ys_kcp_latency", "ys_kcp_concurrent")) { Merge-GroupIntoMerged $g }
                Remove-Item -Recurse -Force $CriterionDir
                Rename-Item -Path $CriterionMerged -NewName "criterion"
            }
        } elseif ($Lib -eq 'kcp_sys') {
            $libclangAvailable = $false
            if ($env:LIBCLANG_PATH) {
                $libclangAvailable = Test-Path (Join-Path $env:LIBCLANG_PATH "libclang.dll") -ErrorAction SilentlyContinue
                if (-not $libclangAvailable) { $libclangAvailable = Test-Path (Join-Path $env:LIBCLANG_PATH "clang.dll") -ErrorAction SilentlyContinue }
            }
            if (-not $libclangAvailable -and $env:Path) {
                foreach ($dir in $env:Path -split ';') {
                    if (Test-Path (Join-Path $dir "libclang.dll") -ErrorAction SilentlyContinue) { $libclangAvailable = $true; break }
                    if (Test-Path (Join-Path $dir "clang.dll") -ErrorAction SilentlyContinue) { $libclangAvailable = $true; break }
                }
            }
            if (-not $libclangAvailable) { Write-Error "libclang not found; set LIBCLANG_PATH or add LLVM/bin to PATH." }
            if (Test-Path $CriterionDir) {
                if (Test-Path $CriterionMerged) { Remove-Item -Recurse -Force $CriterionMerged }
                Copy-Item -Recurse -Force $CriterionDir $CriterionMerged
            }
            Write-Host "Running benchmarks (kcp-sys)..."
            & cargo bench --features kcp-sys 2>&1 | Out-Host
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            if (Test-Path $CriterionMerged) {
                Merge-GroupIntoMerged "kcp_sys_throughput"
                Merge-GroupIntoMerged "kcp_sys_latency"
                Remove-Item -Recurse -Force $CriterionDir
                Rename-Item -Path $CriterionMerged -NewName "criterion"
            }
        } elseif ($Lib -eq 'slipstream_picoquic') {
            if (Test-Path $CriterionDir) {
                if (Test-Path $CriterionMerged) { Remove-Item -Recurse -Force $CriterionMerged }
                Copy-Item -Recurse -Force $CriterionDir $CriterionMerged
            }
            Write-Host "Running benchmarks (slipstream-picoquic only)..."
            & cargo bench --features slipstream-picoquic -- slipstream 2>&1 | Out-Host
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            if (Test-Path $CriterionMerged) {
                foreach ($g in @("slipstream_throughput", "slipstream_latency", "slipstream_concurrent")) { Merge-GroupIntoMerged $g }
                Remove-Item -Recurse -Force $CriterionDir
                Rename-Item -Path $CriterionMerged -NewName "criterion"
            }
        } elseif ($Lib -eq 'quinn') {
            if (Test-Path $CriterionDir) {
                if (Test-Path $CriterionMerged) { Remove-Item -Recurse -Force $CriterionMerged }
                Copy-Item -Recurse -Force $CriterionDir $CriterionMerged
            }
            Write-Host "Running benchmarks (quinn only)..."
            & cargo bench -- quinn 2>&1 | Out-Host
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            if (Test-Path $CriterionMerged) {
                foreach ($g in @("quinn_throughput", "quinn_latency", "quinn_concurrent")) { Merge-GroupIntoMerged $g }
                Remove-Item -Recurse -Force $CriterionDir
                Rename-Item -Path $CriterionMerged -NewName "criterion"
            }
        } elseif ($Lib -eq 'kcprs') {
            if (Test-Path $CriterionDir) {
                if (Test-Path $CriterionMerged) { Remove-Item -Recurse -Force $CriterionMerged }
                Copy-Item -Recurse -Force $CriterionDir $CriterionMerged
            }
            Write-Host "Running benchmarks (kcprs only)..."
            & cargo bench -- kcprs 2>&1 | Out-Host
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            if (Test-Path $CriterionMerged) {
                foreach ($g in @("kcprs_throughput", "kcprs_latency", "kcprs_concurrent")) { Merge-GroupIntoMerged $g }
                Remove-Item -Recurse -Force $CriterionDir
                Rename-Item -Path $CriterionMerged -NewName "criterion"
            }
        }
    } finally {
        Pop-Location
    }
}

if (-not (Test-Path $CriterionDir)) {
    Write-Error "Criterion output not found at $CriterionDir. Run benchmarks first (omit -SkipRun)."
}

# Collect results: group_id, function_id, mean_ns, throughput_bytes
$results = @()
$groups = Get-ChildItem -Path $CriterionDir -Directory -ErrorAction SilentlyContinue
foreach ($groupDir in $groups) {
    $groupId = $groupDir.Name
    $benches = Get-ChildItem -Path $groupDir.FullName -Directory -ErrorAction SilentlyContinue
    foreach ($benchDir in $benches) {
        $functionId = $benchDir.Name
        $newDir = Join-Path $benchDir.FullName "new"
        $benchJson = Join-Path $newDir "benchmark.json"
        $estimJson = Join-Path $newDir "estimates.json"
        if (-not (Test-Path $benchJson) -or -not (Test-Path $estimJson)) { continue }

        $bench = Get-Content $benchJson -Raw | ConvertFrom-Json
        $estim = Get-Content $estimJson -Raw | ConvertFrom-Json
        $meanNs = $estim.mean.point_estimate
        $throughputBytes = $null
        if ($bench.throughput.PSObject.Properties.Name -contains "Bytes") {
            $throughputBytes = $bench.throughput.Bytes
        }
        $results += [PSCustomObject]@{
            GroupId        = $groupId
            FunctionId     = $functionId
            MeanNs         = $meanNs
            ThroughputBytes = $throughputBytes
        }
    }
}

# Parse success rate log (group\tfunction\tsuccess per line; success 0 or 1)
$successRates = @{}
if (Test-Path $SuccessLogPath) {
    Get-Content $SuccessLogPath -ErrorAction SilentlyContinue | ForEach-Object {
        $parts = $_ -split "`t"
        if ($parts.Count -ge 3) {
            $key = "$($parts[0])_$($parts[1])"
            if (-not $successRates.ContainsKey($key)) {
                $successRates[$key] = @{ success = 0; total = 0 }
            }
            $successRates[$key].total += 1
            if ($parts[2] -eq "1") { $successRates[$key].success += 1 }
        }
    }
}

function Format-Time($ns) {
    if ($ns -ge 1e9) { return "{0:N2} s" -f ($ns / 1e9) }
    if ($ns -ge 1e6) { return "{0:N2} ms" -f ($ns / 1e6) }
    if ($ns -ge 1e3) { return "{0:N2} µs" -f ($ns / 1e3) }
    return "$([math]::Round($ns, 2)) ns"
}

function Format-Throughput($bytes, $ns) {
    if (-not $bytes -or $ns -le 0) { return "" }
    $sec = $ns / 1e9
    $bytesPerSec = $bytes / $sec
    if ($bytesPerSec -ge 1GB) { return "{0:N2} GiB/s" -f ($bytesPerSec / 1GB) }
    if ($bytesPerSec -ge 1MB) { return "{0:N2} MiB/s" -f ($bytesPerSec / 1MB) }
    if ($bytesPerSec -ge 1KB) { return "{0:N2} KiB/s" -f ($bytesPerSec / 1KB) }
    return "$([math]::Round($bytesPerSec, 2)) B/s"
}

# Collect hardware info for benchmark context (Windows: CIM; Linux: /proc when available)
function Get-HardwareInfo {
    $parts = @()
    if ($IsWindows -ne $false) {
        # Windows: CIM (works on PowerShell Core and Windows PowerShell)
        try {
            $cpu = Get-CimInstance -ClassName Win32_Processor -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($cpu) {
                $name = ($cpu.Name -replace '\s+', ' ').Trim()
                $cores = $cpu.NumberOfCores
                $logical = $cpu.NumberOfLogicalProcessors
                if ($cores -and $logical) {
                    $parts += "$name ($cores cores / $logical logical)"
                } else {
                    $parts += $name
                }
            }
            $cs = Get-CimInstance -ClassName Win32_ComputerSystem -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($cs -and $cs.TotalPhysicalMemory) {
                $gb = [math]::Round($cs.TotalPhysicalMemory / 1GB, 2)
                $parts += "${gb} GB RAM"
            }
            $os = Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($os) {
                $parts += $os.Caption
            }
        } catch {
            $parts += "Windows (hardware details unavailable)"
        }
    } else {
        # Linux / macOS: best-effort from common sources
        try {
            if (Test-Path "/proc/cpuinfo") {
                $model = Get-Content "/proc/cpuinfo" -ErrorAction SilentlyContinue | Where-Object { $_ -match "model name\s*:\s*(.+)" } | Select-Object -First 1
                if ($model -match "model name\s*:\s*(.+)") {
                    $parts += $Matches[1].Trim()
                }
            }
            if (Test-Path "/proc/meminfo") {
                $mem = Get-Content "/proc/meminfo" -ErrorAction SilentlyContinue | Where-Object { $_ -match "MemTotal:\s+(\d+)" } | Select-Object -First 1
                if ($mem -match "MemTotal:\s+(\d+)") {
                    $kb = [long]$Matches[1]
                    $gb = [math]::Round($kb / 1MB, 2)
                    $parts += "${gb} GB RAM"
                }
            }
            if ($env:OS) { $parts += $env:OS } elseif (Test-Path "/etc/os-release") {
                $osLine = Get-Content "/etc/os-release" -ErrorAction SilentlyContinue | Where-Object { $_ -match "^PRETTY_NAME=" } | Select-Object -First 1
                if ($osLine -match 'PRETTY_NAME="(.+)"') { $parts += $Matches[1] }
            }
        } catch {
            $parts += "Hardware details unavailable"
        }
    }
    if ($parts.Count -eq 0) { return "Hardware details unavailable" }
    return ($parts -join ", ")
}

# Human-friendly payload label for comparison tables
function Get-PayloadLabel($functionId) {
    $map = @{
        "echo_64b"   = "64 B"
        "echo_256b"  = "256 B"
        "echo_1024b" = "1 KiB"
        "echo_4096b" = "4 KiB"
        "echo_8192b" = "8 KiB"
        "echo_rtt_64b" = "64 B RTT"
    }
    if ($map.ContainsKey($functionId)) { return $map[$functionId] }
    return $functionId
}

# Compute throughput in bytes/sec for comparison (higher is better)
function Get-ThroughputBps($bytes, $ns) {
    if (-not $bytes -or $ns -le 0) { return $null }
    return $bytes / ($ns / 1e9)
}

# Get success rate string (e.g. "100%" or "98.5%") for (groupId, functionId)
function Get-SuccessRatePercent($groupId, $functionId) {
    $key = "${groupId}_${functionId}"
    if (-not $successRates.ContainsKey($key)) { return '-' }
    $s = $successRates[$key].success
    $t = $successRates[$key].total
    if ($t -le 0) { return '-' }
    $pct = [math]::Round(100.0 * $s / $t, 1)
    return "${pct}%"
}

# Get success rate for latency row: prefer echo_rtt_64b for this group, else fall back to throughput echo_64b (same 64B payload)
function Get-LatencySuccessRatePercent($latencyGroupId) {
    $pct = Get-SuccessRatePercent $latencyGroupId "echo_rtt_64b"
    if ($pct -ne '-') { return $pct }
    $throughputGroupId = $latencyGroupId -replace '_latency$', '_throughput'
    return Get-SuccessRatePercent $throughputGroupId "echo_64b"
}

# Build markdown tables by group (order: tokio, deepseek, kcprs, ys_kcp, kcp_sys; each throughput/latency/concurrent)
$groupOrder = @(
    "kcp_tokio_throughput",
    "quinn_throughput",
    "slipstream_throughput",
    "kcp_deepseek_throughput",
    "kcprs_throughput",
    "ys_kcp_throughput",
    "kcp_sys_throughput",
    "kcp_tokio_latency",
    "quinn_latency",
    "slipstream_latency",
    "kcp_deepseek_latency",
    "kcprs_latency",
    "ys_kcp_latency",
    "kcp_sys_latency",
    "kcp_tokio_concurrent",
    "quinn_concurrent",
    "slipstream_concurrent",
    "kcp_deepseek_concurrent",
    "kcprs_concurrent",
    "ys_kcp_concurrent"
)

$sb = [System.Text.StringBuilder]::new()
[void]$sb.AppendLine("")
[void]$sb.AppendLine("Generated by ``scripts/bench_to_readme.ps1``. Run ``cargo bench`` then this script to refresh.")
[void]$sb.AppendLine("")
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
[void]$sb.AppendLine("**Last updated:** $timestamp")
$hardwareInfo = Get-HardwareInfo
[void]$sb.AppendLine("**Hardware:** $hardwareInfo")
[void]$sb.AppendLine("")

# ---- Comparison tables: throughput (same payload across implementations) ----
$throughputGroups = @(
    @{ Id = "kcp_tokio_throughput"; Label = "kcp_tokio (UDP)" },
    @{ Id = "quinn_throughput"; Label = "quinn (QUIC)" },
    @{ Id = "slipstream_throughput"; Label = "slipstream_picoquic" },
    @{ Id = "kcp_deepseek_throughput"; Label = "kcp_deepseek" },
    @{ Id = "kcprs_throughput"; Label = "kcprs" },
    @{ Id = "ys_kcp_throughput"; Label = "ys_kcp" },
    @{ Id = "kcp_sys_throughput"; Label = "kcp_sys" }
)
$payloadOrder = @("echo_64b", "echo_256b", "echo_1024b", "echo_4096b", "echo_8192b")
$comparisonRows = @()
foreach ($payload in $payloadOrder) {
    $row = [ordered]@{ Payload = $payload; PayloadLabel = Get-PayloadLabel $payload }
    $timeValues = @{}
    $throughputBps = @{}
    foreach ($gr in $throughputGroups) {
        $r = $results | Where-Object { $_.GroupId -eq $gr.Id -and $_.FunctionId -eq $payload } | Select-Object -First 1
        $timeStr = if ($r) { Format-Time $r.MeanNs } else { '-' }
        $row[$gr.Label] = $timeStr
        $thrStr = if ($r) { Format-Throughput $r.ThroughputBytes $r.MeanNs } else { '-' }
        if (-not $thrStr) { $thrStr = '-' }
        $row["$($gr.Label)_thr"] = $thrStr
        if ($r) {
            $timeValues[$gr.Label] = $r.MeanNs
            $throughputBps[$gr.Label] = Get-ThroughputBps $r.ThroughputBytes $r.MeanNs
        } else {
            $timeValues[$gr.Label] = $null
            $throughputBps[$gr.Label] = $null
        }
    }
    $row._timeValues = $timeValues
    $row._throughputBps = $throughputBps
    $comparisonRows += $row
}

if ($comparisonRows.Count -gt 0) {
    [void]$sb.AppendLine("### Comparison: time per roundtrip (lower is better)")
    [void]$sb.AppendLine("")
    $headers = @("Payload") + ($throughputGroups | ForEach-Object { $_.Label })
    [void]$sb.AppendLine('| ' + ($headers -join ' | ') + ' |')
    [void]$sb.AppendLine('| --- |' + (($throughputGroups | ForEach-Object { ' ---: ' }) -join '|') + '|')
    foreach ($row in $comparisonRows) {
        $cells = @( $row.PayloadLabel )
        foreach ($gr in $throughputGroups) {
            $val = $row[$gr.Label]
            $ns = $row._timeValues[$gr.Label]
            $minNs = ($row._timeValues.Values | Where-Object { $null -ne $_ } | Measure-Object -Minimum).Minimum
            $isBest = ($null -ne $ns -and $null -ne $minNs -and $ns -le $minNs * 1.0001)
            $cells += if ($isBest -and $val -ne '-') { "**$val**" } else { $val }
        }
        [void]$sb.AppendLine('| ' + ($cells -join ' | ') + ' |')
    }
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("### Comparison: throughput (higher is better)")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine('| ' + ($headers -join ' | ') + ' |')
    [void]$sb.AppendLine('| --- |' + (($throughputGroups | ForEach-Object { ' ---: ' }) -join '|') + '|')
    foreach ($row in $comparisonRows) {
        $cells = @( $row.PayloadLabel )
        $maxBps = ($row._throughputBps.Values | Where-Object { $null -ne $_ } | Measure-Object -Maximum).Maximum
        foreach ($gr in $throughputGroups) {
            $thrStr = $row["$($gr.Label)_thr"]
            $bps = $row._throughputBps[$gr.Label]
            $isBest = ($null -ne $bps -and $null -ne $maxBps -and $bps -ge $maxBps * 0.9999 -and $thrStr -ne '-')
            $cells += if ($isBest) { "**$thrStr**" } else { $thrStr }
        }
        [void]$sb.AppendLine('| ' + ($cells -join ' | ') + ' |')
    }
    [void]$sb.AppendLine("")
    # Success rate table (when log was produced)
    $anySuccess = $false
    foreach ($gr in $throughputGroups) {
        foreach ($payload in $payloadOrder) {
            if ($successRates.ContainsKey("$($gr.Id)_$payload")) { $anySuccess = $true; break }
        }
        if ($anySuccess) { break }
    }
    if ($anySuccess) {
        [void]$sb.AppendLine("### Comparison: success rate (higher is better)")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine('| ' + ($headers -join ' | ') + ' |')
        [void]$sb.AppendLine('| --- |' + (($throughputGroups | ForEach-Object { ' ---: ' }) -join '|') + '|')
        foreach ($payload in $payloadOrder) {
            $cells = @( Get-PayloadLabel $payload )
            foreach ($gr in $throughputGroups) {
                $cells += Get-SuccessRatePercent $gr.Id $payload
            }
            [void]$sb.AppendLine('| ' + ($cells -join ' | ') + ' |')
        }
        [void]$sb.AppendLine("")
    }
}

# ---- Comparison: latency (echo_rtt_64b) ----
$latencyGroups = @(
    @{ Id = "kcp_tokio_latency"; Label = "kcp_tokio (UDP)" },
    @{ Id = "quinn_latency"; Label = "quinn (QUIC)" },
    @{ Id = "slipstream_latency"; Label = "slipstream_picoquic" },
    @{ Id = "kcp_deepseek_latency"; Label = "kcp_deepseek" },
    @{ Id = "kcprs_latency"; Label = "kcprs" },
    @{ Id = "ys_kcp_latency"; Label = "ys_kcp" },
    @{ Id = "kcp_sys_latency"; Label = "kcp_sys" }
)
$latencyRow = [ordered]@{ Benchmark = "echo_rtt_64b"; Label = Get-PayloadLabel "echo_rtt_64b" }
$latencyNs = @{}
foreach ($gr in $latencyGroups) {
    $r = $results | Where-Object { $_.GroupId -eq $gr.Id -and $_.FunctionId -eq "echo_rtt_64b" } | Select-Object -First 1
    $latencyRow[$gr.Label] = if ($r) { Format-Time $r.MeanNs } else { '-' }
    $latencyNs[$gr.Label] = if ($r) { $r.MeanNs } else { $null }
}
$minLatencyNs = ($latencyNs.Values | Where-Object { $null -ne $_ } | Measure-Object -Minimum).Minimum
[void]$sb.AppendLine("### Comparison: latency (64 B RTT, lower is better)")
[void]$sb.AppendLine("")
$latHeaders = @("Benchmark") + ($latencyGroups | ForEach-Object { $_.Label })
[void]$sb.AppendLine('| ' + ($latHeaders -join ' | ') + ' |')
[void]$sb.AppendLine('| --- |' + (($latencyGroups | ForEach-Object { ' ---: ' }) -join '|') + '|')
$latCells = @( $latencyRow.Label )
foreach ($gr in $latencyGroups) {
    $val = $latencyRow[$gr.Label]
    $ns = $latencyNs[$gr.Label]
    $isBest = ($null -ne $ns -and $null -ne $minLatencyNs -and $ns -le $minLatencyNs * 1.0001 -and $val -ne '-')
    $latCells += if ($isBest) { "**$val**" } else { $val }
}
[void]$sb.AppendLine('| ' + ($latCells -join ' | ') + ' |')
# Success rate row for latency (echo_rtt_64b); fallback to throughput 64B success when latency log missing
$latSuccessRow = @( "Success rate" )
foreach ($gr in $latencyGroups) {
    $latSuccessRow += Get-LatencySuccessRatePercent $gr.Id
}
[void]$sb.AppendLine('| ' + ($latSuccessRow -join ' | ') + ' |')
[void]$sb.AppendLine("")

# Per-group tables omitted in README (comparison tables above are sufficient)

$newSection = $sb.ToString()

# Read README and replace ## Benchmark results ... (until next ## or EOF)
$readmeContent = Get-Content $ReadmePath -Raw
if ($readmeContent -match '(?s)## Benchmark results\r?\n') {
    # Replace from "## Benchmark results" until the next "## " (keep that next heading)
    $pattern = '(?s)(## Benchmark results\r?\n).*?(\r?\n## [^\r\n]+)'
    $readmeContent = $readmeContent -replace $pattern, ('${1}' + $newSection.TrimEnd() + "`n" + '${2}')
} else {
    # Insert before ## Dependencies
    $readmeContent = $readmeContent -replace '(\r?\n## Dependencies)', ("`n`n" + $newSection.TrimEnd() + "`n`n`$1")
}

Set-Content -Path $ReadmePath -Value $readmeContent.TrimEnd() -NoNewline
Write-Host "Updated README with benchmark results."
