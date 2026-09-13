@echo off
cd /d "%~dp0"
if not exist "dist\kaflow-windows-amd64.exe" (
  echo Missing dist\kaflow-windows-amd64.exe
  pause
  exit /b 1
)
start "Kaflow" "dist\kaflow-windows-amd64.exe"
echo Kaflow is starting. The browser will open shortly.
