@echo off
setlocal enabledelayedexpansion

REM Full pipeline for capaStudy
set "BASE_DIR=D:\Files\demo\capaStudy"
set "MSC_DIR=%BASE_DIR%\MSC FETCH"
set "MSK_DIR=%BASE_DIR%\MSK FETCH"
set "CSL_DIR=%BASE_DIR%\CSL FETCH"
set "MERGE_SCRIPT=%BASE_DIR%\merge_all_carriers.py"

echo ========================================
echo [START] Full query pipeline
echo Base: %BASE_DIR%
echo Time: %DATE% %TIME%
echo ========================================

echo.
echo [1/4] Run MSC full query...
pushd "%MSC_DIR%"
python "%MSC_DIR%\MSC_FETCH.py"
if errorlevel 1 (
  echo [ERROR] MSC full query failed.
  popd
  exit /b 1
)
popd

echo.
echo [2/4] Run MSK full query...
pushd "%MSK_DIR%"
python "%MSK_DIR%\MSK_FETCH.py"
if errorlevel 1 (
  echo [ERROR] MSK full query failed.
  popd
  exit /b 1
)
popd

echo.
echo [3/4] Run CSL full query...
pushd "%CSL_DIR%"
python "%CSL_DIR%\CSL_FETCH.py"
if errorlevel 1 (
  echo [ERROR] CSL full query failed.
  popd
  exit /b 1
)
popd

echo.
echo [4/4] Merge latest outputs...
pushd "%BASE_DIR%"
python "%MERGE_SCRIPT%"
if errorlevel 1 (
  echo [ERROR] Merge failed.
  popd
  exit /b 1
)
popd

echo.
echo ========================================
echo [DONE] Full query pipeline finished.
echo Time: %DATE% %TIME%
echo ========================================

exit /b 0
