$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python 3.10+ is required. Install it from https://www.python.org/downloads/ and rerun this script."
}

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

& ".\.venv\Scripts\python.exe" -m pip install --upgrade pip
& ".\.venv\Scripts\python.exe" -m pip install -r requirements.txt

if (Get-Command npx -ErrorAction SilentlyContinue) {
    npx --yes skills add GMGNAI/gmgn-skills --yes --global
} else {
    Write-Warning "Node.js/npx was not found, so GMGN skills were not installed. Install Node.js and run: npx skills add GMGNAI/gmgn-skills"
}

Write-Host "Setup complete. Run .\run_windows.ps1 and open http://127.0.0.1:8000"
