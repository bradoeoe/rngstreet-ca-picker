@echo off
setlocal

set "ROOT=%~dp0"
set "PYTHON_EXE=%ROOT%.venv\Scripts\python.exe"

if not exist "%PYTHON_EXE%" (
  echo Could not find "%PYTHON_EXE%".
  echo Create the virtual environment first.
  exit /b 1
)

start "RNG CA Bot" cmd /k "cd /d ""%ROOT%"" && ""%PYTHON_EXE%"" -m rng_ca_bot.main"
start "RNG CA Backend" cmd /k "cd /d ""%ROOT%"" && ""%PYTHON_EXE%"" frontend\server\app.py"
start "RNG CA Frontend" cmd /k "cd /d ""%ROOT%frontend\client"" && npm run dev"

endlocal
