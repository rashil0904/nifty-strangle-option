# Runs one strategy script: loads .env into the process environment, cd's to the
# repo root (state/log files are written relative to cwd), and appends the
# script's stdout/stderr to cron_<name>.log. Paper mode unless -Live is passed.
param(
    [Parameter(Mandatory)][string]$Script,   # e.g. Nifty\strategy\delta_strangle_strategy.py
    [Parameter(Mandatory)][string]$LogName,  # e.g. nifty_strangle
    [string]$StateFile = "",
    [switch]$Live
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

# Load KEY=VALUE lines from .env (skips blanks/comments; value kept literally, so '$' is safe).
Get-Content (Join-Path $repo ".env") | ForEach-Object {
    if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
        Set-Item -Path "env:$($Matches[1])" -Value $Matches[2]
    }
}

$python = (Get-Command python -ErrorAction Stop).Source
$argList = @($Script)
if ($Live) { $argList += "--live" }
if ($StateFile) { $argList += @("--state-file", $StateFile) }

$log = Join-Path $repo ("cron_{0}_{1}.log" -f $LogName, (Get-Date -Format "yyyy-MM-dd"))
# Start-Process with file redirects: the scripts log to stderr, and PowerShell 5.1's own `*>>` turns
# redirected native stderr into errors (fatal under $ErrorActionPreference=Stop), which silently
# killed the process after its first log line.
Start-Process -FilePath $python -ArgumentList $argList -WorkingDirectory $repo -NoNewWindow -Wait `
    -RedirectStandardOutput $log -RedirectStandardError ($log -replace '\.log$', '.err.log')
