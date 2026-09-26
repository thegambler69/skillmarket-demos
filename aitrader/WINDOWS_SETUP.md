# Windows setup

This repository contains all application source code. Python packages, caches, logs, private keys, and runtime trading state are intentionally rebuilt or configured locally rather than committed.

## Requirements

- Python 3.10 or newer
- Node.js with npx (needed to install the GMGN skills)
- Git

## Install

From PowerShell:

```powershell
git clone https://github.com/thegambler69/skillmarket-demos.git
cd skillmarket-demos\aitrader
.\setup_windows.ps1
```

The setup script creates an isolated Python environment, installs the application requirements, and runs:

```powershell
npx --yes skills add GMGNAI/gmgn-skills --yes --global
```

## Run

```powershell
.\run_windows.ps1
```

Open http://127.0.0.1:8000.

The app starts in mock/shadow mode without credentials. For live market access or trading, configure credentials separately on the new computer in `~/.config/gmgn/.env`. Never commit that file or any private key.
