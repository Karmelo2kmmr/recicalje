@echo off
echo Stopping Arbitrage Hammer and CLOB daemon...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and (($_.Name -match '^python(\.exe)?$' -and $_.CommandLine -match 'clob_daemon.py') -or $_.Name -eq 'arbitrage_hammer.exe') } | ForEach-Object { Write-Host ('Stopping PID ' + $_.ProcessId + ' ' + $_.Name); Stop-Process -Id $_.ProcessId -Force }"
echo Done.
pause
