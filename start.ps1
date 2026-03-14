$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Write-Host "Creating virtual environment..."
    python -m venv .venv
}

$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"

Write-Host "Installing dependencies..."
& $pythonExe -m pip install -r requirements.txt

Write-Host "Starting Intraday Stock Scanner..."
& $pythonExe main.py
