# Switches the system to the single windowless supervisor (see supervisor.py).
#   .\scheduler\register_supervisor.ps1           # register the supervisor + DISABLE (not delete) the old per-strategy tasks
#   .\scheduler\register_supervisor.ps1 -Remove   # unregister the supervisor and re-enable the old tasks
# Old tasks are disabled rather than removed so that processes they already started are not killed.
param([switch]$Remove)
$name = "NiftyStrangle-Supervisor"
$old = @("NiftyStrangle-Carryover","NiftyStrangle-Nifty-Strangle","NiftyStrangle-Sensex-Strangle","NiftyStrangle-Nifty-Breakdown",
         "NiftyStrangle-Sensex-Breakdown","NiftyStrangle-Sensex-Breakdown-2")
if ($Remove) {
    Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
    $old | ForEach-Object { Enable-ScheduledTask -TaskName $_ -ErrorAction SilentlyContinue | Out-Null }
    Write-Host "Removed $name; old tasks re-enabled"; return
}
$repo = Split-Path -Parent $PSScriptRoot
$pythonw = Join-Path (Split-Path (Get-Command python -ErrorAction Stop).Source) "pythonw.exe"
if (-not (Test-Path $pythonw)) { throw "pythonw.exe not found next to python.exe" }
$action = New-ScheduledTaskAction -Execute $pythonw -Argument "scheduler\supervisor.py" -WorkingDirectory $repo
$daily = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "09:00"
$logon = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$startup = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1)
try {
    Register-ScheduledTask -TaskName $name -Action $action -Trigger @($daily,$logon,$startup) -Settings $settings -Force -ErrorAction Stop | Out-Null
    Write-Host "Registered ${name}: 09:00 Mon-Fri + at logon + at startup"
} catch {
    Write-Host "At-startup trigger needs admin rights ($($_.Exception.Message.Trim())) -- registering 09:00 + at-logon only"
    Register-ScheduledTask -TaskName $name -Action $action -Trigger @($daily,$logon) -Settings $settings -Force | Out-Null
    Write-Host "Registered ${name}: 09:00 Mon-Fri + at logon"
}
$old | ForEach-Object { Disable-ScheduledTask -TaskName $_ -ErrorAction SilentlyContinue | Out-Null; Write-Host "Disabled $_" }
Start-ScheduledTask -TaskName $name
Write-Host "Started $name"

