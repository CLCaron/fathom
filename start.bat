@echo off
cd /d "%~dp0"
start /b python -m uvicorn fathom.main:app --host 127.0.0.1 --port 8000
timeout /t 3 /nobreak >nul
start http://127.0.0.1:8000
