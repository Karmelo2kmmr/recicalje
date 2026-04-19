@echo off
set RUST_LOG=info
cd /d "C:\Users\daniel\OneDrive\Desktop\bots poly\arbitrage_hammer"
echo Starting Arbitrage Hammer in background...
start /B cargo run --release > arbitrage_hammer.log 2>&1
echo Bot is running. Check arbitrage_hammer.log for updates.
pause
