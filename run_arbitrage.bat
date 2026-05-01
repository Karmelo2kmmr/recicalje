@echo off
set RUST_LOG=info
cd /d "C:\Users\daniel\OneDrive\Desktop\bots poly\arbitrage_hammer"
echo Building release binary...
cargo build --release
if errorlevel 1 exit /b 1
echo Starting Arbitrage Hammer and CLOB daemon...
powershell -NoProfile -ExecutionPolicy Bypass -File ".\launch_all.ps1"
echo Check arbitrage_hammer.err.log and clob_daemon.err.log for updates.
pause
