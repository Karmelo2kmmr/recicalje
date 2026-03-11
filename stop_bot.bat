@echo off
title Stop DCA+Reciclaje Bot
cd /d "%~dp0"

if not exist bot_pid.txt (
    echo No se encontro bot_pid.txt. El bot no parece estar corriendo.
    pause
    exit /b 0
)

set /p PID=<bot_pid.txt

echo Cerrando bot con PID: %PID%...
taskkill /PID %PID% /F >NUL 2>&1

if errorlevel 1 (
    echo El proceso ya no existia o no se pudo cerrar.
) else (
    echo Bot detenido correctamente.
)

del bot_pid.txt >NUL 2>&1
echo Archivo bot_pid.txt eliminado.
pause
