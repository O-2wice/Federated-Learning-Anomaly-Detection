@echo off
REM ============================================================================
REM FLEAD Platform Startup Script (Windows)
REM Single-broker Kafka mode - starts all Docker services and pipeline
REM Usage: START.bat [--fast] [--no-wait] [--safe] [--no-browser]
REM ============================================================================
setlocal enabledelayedexpansion
title FEDERATED LEARNING PLATFORM - COMPLETE PIPELINE STARTUP

REM Ensure we run from repo root (folder containing this file)
cd /d "%~dp0"

echo.
echo ====================================================================
echo FEDERATED LEARNING PLATFORM - COMPLETE PIPELINE STARTUP (WINDOWS)
echo ====================================================================
echo.

REM ---------------------------------------------------
REM STEP 1: CHECK PYTHON
REM ---------------------------------------------------
echo Step 1: Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found in PATH.
    echo Please install Python and ensure 'python' works in this terminal.
    pause
    exit /b 1
)
echo OK
echo.

REM ---------------------------------------------------
REM STEP 2: CHECK DOCKER
REM ---------------------------------------------------
echo Step 2: Checking Docker...
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker not found in PATH or Docker Desktop not running.
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)
echo OK
echo.

REM ---------------------------------------------------
REM STEP 3: START JUPYTER DEV UI (OPTIONAL)
REM ---------------------------------------------------
echo Starting optional Jupyter UI (dev) container...
docker compose up -d jupyter-dev >nul 2>&1
echo   Jupyter UI (if started) at: http://localhost:8888
echo.

REM ---------------------------------------------------
REM STEP 4: DATA PREPROCESSING IN DOCKER (KAGGLE -> CSVs)
REM ---------------------------------------------------
echo Step 3: Running data preprocessor (Kaggle) in Docker...

set "DEVICE_CSV=data\processed\device_0.csv"
set "CHUNK_FILE=data\processed\chunks\X_chunk_0.npz"

REM If preprocessed data already exists, skip
IF EXIST "%DEVICE_CSV%" (
    echo   Preprocessed data already exists ^(device CSV: %DEVICE_CSV%^). Skipping data preprocessing...
) ELSE (
    IF EXIST "%CHUNK_FILE%" (
        echo   Preprocessed data already exists ^(chunk file: %CHUNK_FILE%^). Skipping data preprocessing...
    ) ELSE (
        echo   No preprocessed data found, running Docker data-preprocessor...

        IF NOT EXIST "kaggle\kaggle.json" (
            echo [ERROR] kaggle\kaggle.json not found.
            echo Please place your Kaggle API token file in:
            echo   kaggle\kaggle.json
            pause
            exit /b 1
        )

        docker compose run --rm data-preprocessor
        IF ERRORLEVEL 1 (
            echo [WARNING] Docker data-preprocessor returned a non-zero exit code.

            IF EXIST "%DEVICE_CSV%" (
                echo   Device CSVs found in data\processed\*.csv. Treating preprocessing as successful.
            ) ELSE (
                IF EXIST "%CHUNK_FILE%" (
                    echo   Chunk files found in data\processed\chunks. Treating preprocessing as successful.
                ) ELSE (
                    echo [ERROR] No device CSVs or chunk files were generated. Cannot continue.
                    pause
                    exit /b 1
                )
            )
        )
    )
)
echo OK
echo.

REM ---------------------------------------------------
REM STEP 5: CLEANUP PHASE
REM ---------------------------------------------------
echo.
echo ====================================================================
echo CLEANUP PHASE (WINDOWS)
echo ====================================================================
echo.

echo Cleaning up old Docker containers...
docker compose down --remove-orphans >nul 2>&1

REM Extra hard cleanup (safe even if some containers don't exist).
REM spark-analytics is the old name of dashboard-metrics-updater.
docker container rm -f zookeeper kafka timescaledb grafana kafka-ui ^
    flink-jobmanager flink-taskmanager spark-master spark-worker-1 ^
    kafka-broker-1 ^
    timescaledb-collector federated-aggregator device-viewer ^
    monitoring-dashboard grafana-init database-init data-preprocessor ^
    spark-analytics dashboard-metrics-updater >nul 2>&1

echo Done
echo.

REM ---------------------------------------------------
REM STEP 6: DOCKER SERVICES STARTUP
REM ---------------------------------------------------
echo ====================================================================
echo DOCKER SERVICES STARTUP (WINDOWS)
echo ====================================================================
echo.

echo Starting Docker containers (rebuilding images whose Dockerfile or requirements changed)...
docker compose up -d --build
echo.

echo Waiting 30 seconds for services to come up (single-broker setup)...
timeout /t 30 >nul
docker compose ps
echo.

REM ---------------------------------------------------
REM STEP 7: PIPELINE ORCHESTRATOR
REM ---------------------------------------------------
echo ====================================================================
echo STARTING PIPELINE ORCHESTRATOR (WINDOWS)
echo ====================================================================
echo.

REM Handle optional startup flags
SET "ORCH_FLAGS="
FOR %%A IN (%*) DO (
    IF /I "%%~A"=="--fast" (
        SET "ORCH_FLAGS=!ORCH_FLAGS! --fast"
    ) ELSE IF /I "%%~A"=="--no-wait" (
        SET "ORCH_FLAGS=!ORCH_FLAGS! --no-wait"
    ) ELSE IF /I "%%~A"=="--safe" (
        REM Keep --safe as known flag for compatibility (no-op)
        SET "ORCH_FLAGS=!ORCH_FLAGS! --no-wait"
    ) ELSE (
        REM pass unknown args through as-is
        SET "ORCH_FLAGS=!ORCH_FLAGS! %%~A"
    )
)

call python scripts\pipeline_orchestrator.py %ORCH_FLAGS%

REM ---------------------------------------------------
REM STEP 8: FINAL STATUS & DASHBOARDS
REM ---------------------------------------------------
echo.
echo ====================================================================
echo PLATFORM STARTUP COMPLETE - ALL SERVICES IN DOCKER (WINDOWS)
echo ====================================================================
echo.
echo ACCESS POINTS:
echo   Live Monitoring:          http://localhost:5001
echo   Grafana Dashboard:        http://localhost:3001  (admin/admin)
echo   Kafka UI:                 http://localhost:8081
echo   Device Viewer Website:    http://localhost:8082
echo   Flink Dashboard:          http://localhost:8161
echo   Spark Master:             http://localhost:8086
echo   Spark Worker:             http://localhost:8087
echo   Jupyter Dev UI:           http://localhost:8888
echo   Prometheus:               http://localhost:9090
echo   Alertmanager:             http://localhost:9093
echo   TimescaleDB:              localhost:5432
echo.
echo LOGS:
echo   docker compose logs -f
echo.
echo ====================================================================
echo All dashboards opened! Press any key to close this window.
echo ====================================================================
echo.
pause
endlocal
exit /b 0

