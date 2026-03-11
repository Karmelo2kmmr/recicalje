@echo off
title DCA+Reciclaje Bot
cd /d "%~dp0"

REM Verificar si ya hay un proceso corriendo
if exist bot_pid.txt (
    set /p OLD_PID=<bot_pid.txt
    tasklist /FI "PID eq %OLD_PID%" 2>NUL | find /I "poly_rebound" >NUL 2>&1
    if not errorlevel 1 (
        echo El bot ya esta corriendo con PID %OLD_PID%. Usa stop_bot.bat para cerrarlo.
        pause
        exit /b 1
    )
)

REM Configurar logs con fecha
REM Configurar logs con fecha (Robusto con PowerShell)
for /f "usebackq tokens=*" %%i in (`powershell -NoProfile -Command "Get-Date -Format 'yyyy-MM-dd'"`) do set FECHA=%%i
set LOG_OUT=bot_out_%FECHA%.log
set LOG_ERR=bot_err_%FECHA%.log

echo Iniciando DCA+Reciclaje Bot...
echo Logs: %LOG_OUT% / %LOG_ERR%

REM Arrancar en background con Start-Process
powershell -Command "Start-Process -FilePath '.\target\release\poly_rebound.exe' -RedirectStandardOutput '%LOG_OUT%' -RedirectStandardError '%LOG_ERR%' -WindowStyle Hidden -PassThru | Select-Object -ExpandProperty Id | Out-File -FilePath 'bot_pid.txt' -Encoding ASCII"

timeout /t 2 > nul
if exist bot_pid.txt (
    set /p NEW_PID=<bot_pid.txt
    echo.
    echo ============================================
    echo  DCA+Reciclaje Bot arrancado! PID: %NEW_PID%
    echo  Log salida : %LOG_OUT%
    echo  Log errores: %LOG_ERR%
    echo  Para detener: stop_bot.bat
    echo ============================================
    echo.
)
pause
