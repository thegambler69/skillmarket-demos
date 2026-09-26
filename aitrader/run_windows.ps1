$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
    throw "The local environment is missing. Run .\setup_windows.ps1 first."
}

& ".\.venv\Scripts\python.exe" app.py
