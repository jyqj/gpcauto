@echo off
cd /d "%~dp0"
python start.py
if %errorlevel% neq 0 (
    python3 start.py
)
pause
