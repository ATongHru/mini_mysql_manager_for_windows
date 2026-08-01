@echo off
setlocal
cd /d "%~dp0"

echo ==================================================
echo MySQL Manager - Build (no default password)
echo Working directory: %CD%
echo ==================================================
echo.
echo [1/2] Checking Go environment...
go version
if errorlevel 1 goto :failed

echo.
echo [2/2] Building MiniMySQLManager.exe...
go build -v -trimpath -ldflags="-s -w" -o MiniMySQLManager.exe .
if errorlevel 1 goto :failed

echo.
echo ==================================================
echo Build succeeded: %CD%\MiniMySQLManager.exe
echo No database password is included in this build.
echo ==================================================
pause
exit /b 0

:failed
echo.
echo ==================================================
echo Build failed. Review the messages above.
echo ==================================================
pause
exit /b 1
