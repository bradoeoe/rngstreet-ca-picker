@echo off
setlocal

for %%I in ("%~dp0.") do set "ROOT=%%~fI"
set "PYTHON_EXE=%ROOT%\.venv\Scripts\python.exe"
set "FRONTEND_DIR=%ROOT%\frontend\client"
set "PYTHONPATH=%ROOT%"

if not exist "%PYTHON_EXE%" (
  echo Could not find "%PYTHON_EXE%".
  echo Create the virtual environment first.
  exit /b 1
)

if not exist "%FRONTEND_DIR%" (
  echo Could not find "%FRONTEND_DIR%".
  exit /b 1
)

start "RNG CA Bot" /D "%ROOT%" cmd /k "%PYTHON_EXE%" -m rng_ca_bot.main
start "RNG CA Backend" /D "%ROOT%" cmd /k "%PYTHON_EXE%" frontend\server\app.py
start "RNG CA Frontend" /D "%FRONTEND_DIR%" cmd /k "npm run dev -- --host 0.0.0.0"

endlocal
