@echo off
setlocal

if not exist ".venv\Scripts\python.exe" (
    echo ERROR: .venv\Scripts\python.exe was not found.
    echo Please create the virtual environment and install project dependencies first.
    pause
    exit /b 1
)

".venv\Scripts\python.exe" scripts\diagnose_pykrx_daily_download.py
set EXIT_CODE=%ERRORLEVEL%

echo.
echo ========================================
echo Download diagnostics finished.
echo Exit code: %EXIT_CODE%
echo ========================================
pause
exit /b %EXIT_CODE%
