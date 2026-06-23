@echo off
setlocal

if not exist ".venv\Scripts\python.exe" (
    echo ERROR: .venv\Scripts\python.exe was not found.
    echo Please create the virtual environment and install project dependencies first.
    pause
    exit /b 1
)

".venv\Scripts\python.exe" scripts\run_daily_update_pipeline.py --dry-run --skip-download
set EXIT_CODE=%ERRORLEVEL%

echo.
echo ========================================
echo Pipeline finished.
echo Exit code: %EXIT_CODE%
echo.
if "%EXIT_CODE%"=="1" (
    echo Meaning: Failed - production feature source completeness check failed.
) else if "%EXIT_CODE%"=="130" (
    echo Meaning: User interrupted.
) else (
    echo Meaning: Success.
)
echo 0 = success
echo 1 = failed
echo 130 = user interrupted
echo ========================================
pause
exit /b %EXIT_CODE%
