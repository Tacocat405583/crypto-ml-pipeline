@echo off
REM Wrapper for Windows Task Scheduler.
REM Task Scheduler discards stdout/stderr, so both are redirected to a log file.
REM %~dp0 is this file's own directory, so the task works from any working dir.

cd /d "%~dp0"

if not exist "logs" mkdir "logs"

echo [%date% %time%] start >> "logs\tracker.log"
".venv\Scripts\python.exe" "src\tracker\coinbase.py" >> "logs\tracker.log" 2>&1
set RC=%ERRORLEVEL%
echo [%date% %time%] end rc=%RC% >> "logs\tracker.log"

REM propagate the exit code so a failed run shows as failed in Task Scheduler
exit /b %RC%
