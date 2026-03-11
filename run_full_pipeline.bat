@echo off
setlocal

set "BASE_DIR=D:\Files\demo\capaStudy"
set "PIPELINE=%BASE_DIR%\run_pipeline.py"

if not exist "%PIPELINE%" (
  echo [ERROR] Missing file: %PIPELINE%
  exit /b 1
)

python "%PIPELINE%" %*
exit /b %ERRORLEVEL%
