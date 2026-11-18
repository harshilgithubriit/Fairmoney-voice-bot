@echo off
setlocal
title FairMoney VoiceBot - Server Engine
chcp 65001 >nul 2>&1

:: ==============================================================================
:: 1. PORTABLE WORKING DIRECTORY
:: ==============================================================================
cd /d "%~dp0"
set "BOT_DIR=%~dp0"

echo ==============================================================================
echo                FAIRMONEY VOICEBOT - AUTOMATIC STARTUP SYSTEM                  
echo ==============================================================================
echo [INFO] Working Directory: %BOT_DIR%

:: ==============================================================================
:: 2. AUTO-CREATE RUNTIME DIRECTORIES
:: ==============================================================================
if not exist "%BOT_DIR%logs" mkdir "%BOT_DIR%logs" >nul 2>&1
if not exist "%BOT_DIR%transcripts" mkdir "%BOT_DIR%transcripts" >nul 2>&1
if not exist "%BOT_DIR%session" mkdir "%BOT_DIR%session" >nul 2>&1
if not exist "%BOT_DIR%dispositions" mkdir "%BOT_DIR%dispositions" >nul 2>&1
if not exist "%BOT_DIR%data" mkdir "%BOT_DIR%data" >nul 2>&1

:: ==============================================================================
:: 3. RESOLVE VIRTUAL ENVIRONMENT (.venv)
:: ==============================================================================
set "PYTHON="

:: Check if local .venv exists and works on this machine/folder
if exist "%BOT_DIR%.venv\Scripts\python.exe" (
    "%BOT_DIR%.venv\Scripts\python.exe" -c "import sys" >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON=%BOT_DIR%.venv\Scripts\python.exe"
        goto :PYTHON_FOUND
    )
    echo [WARNING] Existing .venv is invalid or was copied from another machine/path.
    echo [INFO] Rebuilding clean .venv for this machine...
    rmdir /s /q "%BOT_DIR%.venv"
)

:: Find a working host Python to create .venv
set "HOST_PYTHON="

:: Try 'py -3' launcher
where py >nul 2>&1
if not errorlevel 1 (
    for /f "delims=" %%I in ('py -3 -c "import sys; print(sys.executable)" 2^>nul') do (
        if not defined HOST_PYTHON if exist "%%I" set "HOST_PYTHON=%%I"
    )
)

:: Try system PATH 'python'
if not defined HOST_PYTHON (
    where python >nul 2>&1
    if not errorlevel 1 (
        for /f "delims=" %%I in ('where python') do (
            if not defined HOST_PYTHON (
                "%%I" -c "import sys" >nul 2>&1
                if not errorlevel 1 set "HOST_PYTHON=%%I"
            )
        )
    )
)

:: Try standard installation paths
if not defined HOST_PYTHON (
    for %%V in (Python313 Python312 Python311 Python310 Python39) do (
        if not defined HOST_PYTHON if exist "%LOCALAPPDATA%\Programs\Python\%%V\python.exe" set "HOST_PYTHON=%LOCALAPPDATA%\Programs\Python\%%V\python.exe"
        if not defined HOST_PYTHON if exist "C:\%%V\python.exe" set "HOST_PYTHON=C:\%%V\python.exe"
        if not defined HOST_PYTHON if exist "C:\Program Files\Python\%%V\python.exe" set "HOST_PYTHON=C:\Program Files\Python\%%V\python.exe"
        if not defined HOST_PYTHON if exist "C:\Program Files\Python%%V\python.exe" set "HOST_PYTHON=C:\Program Files\Python%%V\python.exe"
        if not defined HOST_PYTHON if exist "D:\%%V\python.exe" set "HOST_PYTHON=D:\%%V\python.exe"
    )
)

if not defined HOST_PYTHON (
    echo ==============================================================================
    echo [ERROR] No working Python installation was found on this system!
    echo.
    echo Please install Python 3.10+ from https://www.python.org/downloads/
    echo IMPORTANT: Make sure to check "Add python.exe to PATH" during installation.
    echo ==============================================================================
    echo.
    pause
    exit /b 1
)

echo [INFO] Creating new virtual environment (.venv) using:
echo        %HOST_PYTHON%
"%HOST_PYTHON%" -m venv "%BOT_DIR%.venv"
if not exist "%BOT_DIR%.venv\Scripts\python.exe" (
    echo [ERROR] Failed to create virtual environment in %BOT_DIR%.venv
    pause
    exit /b 1
)

set "PYTHON=%BOT_DIR%.venv\Scripts\python.exe"

echo [INFO] Installing required dependencies from requirements.txt...
"%PYTHON%" -m pip install --upgrade pip
"%PYTHON%" -m pip install -r "%BOT_DIR%requirements.txt"

:PYTHON_FOUND
:: ==============================================================================
:: 4. VERIFY DEPENDENCIES
:: ==============================================================================
"%PYTHON%" -c "import dotenv, websockets, azure.cognitiveservices.speech, langchain_groq" >nul 2>&1
if errorlevel 1 (
    echo [INFO] Installing missing dependencies from requirements.txt...
    "%PYTHON%" -m pip install -r "%BOT_DIR%requirements.txt"
)

:: ==============================================================================
:: 5. VERIFY ENTRY FILE
:: ==============================================================================
if not exist "%BOT_DIR%run_servers.py" (
    echo [ERROR] Entry file run_servers.py not found in:
    echo         %BOT_DIR%
    pause
    exit /b 1
)

:: ==============================================================================
:: 6. LAUNCH RUN_SERVERS
:: ==============================================================================
set "PYTHONPATH=%BOT_DIR%;%PYTHONPATH%"
set "PYTHONUNBUFFERED=1"
set "PYTHONIOENCODING=utf-8"

echo.
echo ==============================================================================
echo [SUCCESS] Environment Ready!
echo Python Interpreter : %PYTHON%
echo AudioSocket Server : 0.0.0.0:9092
echo FastAGI Server     : 0.0.0.0:4573
echo ==============================================================================
echo.
echo Starting VoiceBot servers (Press Ctrl+C to stop)...
echo.

"%PYTHON%" "%BOT_DIR%run_servers.py"

set "EXIT_CODE=%errorlevel%"
echo.
echo ==============================================================================
echo [INFO] VoiceBot servers stopped (Exit code: %EXIT_CODE%).
echo ==============================================================================
pause
exit /b %EXIT_CODE%
