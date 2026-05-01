$ErrorActionPreference = "Stop"
$workdir = "C:\Users\daniel\OneDrive\Desktop\bots poly\arbitrage_hammer"
$env:RUST_LOG = "info"

$existing = Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -and (
        ($_.Name -match "^python(\.exe)?$" -and $_.CommandLine -match "clob_daemon.py") -or
        ($_.Name -eq "arbitrage_hammer.exe")
    )
}

if ($existing) {
    Write-Output "Ya hay procesos del bot corriendo. Usa .\stop_arbitrage.bat antes de relanzar."
    $existing | Select-Object ProcessId,Name,CommandLine | Format-List
    exit 1
}

$clobDaemon = Start-Process -FilePath "python" `
    -ArgumentList "clob_daemon.py" `
    -WindowStyle Hidden `
    -RedirectStandardOutput "clob_daemon.out.log" `
    -RedirectStandardError "clob_daemon.err.log" `
    -PassThru `
    -WorkingDirectory $workdir

Start-Sleep -Seconds 2

& ".\target\release\live_preflight.exe"
if ($LASTEXITCODE -ne 0) {
    Stop-Process -Id $clobDaemon.Id -Force -ErrorAction SilentlyContinue
    Write-Output "Preflight live falló. Daemon detenido; bot no lanzado."
    exit 1
}

$bot = Start-Process -FilePath ".\target\release\arbitrage_hammer.exe" `
    -WindowStyle Hidden `
    -RedirectStandardOutput "arbitrage_hammer.out.log" `
    -RedirectStandardError "arbitrage_hammer.err.log" `
    -PassThru `
    -WorkingDirectory $workdir

Write-Output "Arbitrage Hammer lanzado con exito."
Write-Output "CLOB Daemon PID: $($clobDaemon.Id)"
Write-Output "Bot PID: $($bot.Id)"
Write-Output "Logs bot: Get-Content arbitrage_hammer.err.log -Wait"
Write-Output "Logs daemon: Get-Content clob_daemon.err.log -Wait"
