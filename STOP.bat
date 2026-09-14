@echo off
REM ============================================================================
REM FLEAD Platform Shutdown Script (Windows)
REM Stops all Docker services and cleans up containers
REM Usage: STOP.bat
REM ============================================================================
setlocal
title FEDERATED LEARNING PLATFORM - SHUTDOWN (WINDOWS)

rem Ensure we run from repo root (folder containing this file)
cd /d "%~dp0"

echo.
echo ===================================================
echo FEDERATED LEARNING PLATFORM - SHUTDOWN (WINDOWS)
echo ===================================================
echo.

echo Stopping all services...
echo.

REM Stop and remove ONLY this project's containers. Data volumes (TimescaleDB,
REM Kafka, Grafana, models) are kept for the next START.bat.
REM The previous version killed every container on the machine, pruned every
REM unused Docker volume (wiping other apps' data) and force-killed every
REM python.exe process.
echo Stopping FLEAD containers...
docker compose --profile data down --remove-orphans
echo Done.

REM Containers from older runs that may not be tracked by compose
docker container rm -f data-preprocessor jupyter-dev >nul 2>&1
echo.

echo ===================================================
echo SHUTDOWN COMPLETE (WINDOWS)
echo ===================================================
echo To start again: START.bat
echo.
pause
endlocal
