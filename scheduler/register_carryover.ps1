# Registers the 09:13 carry-over job (Mon-Fri): re-attaches any unfinished cycle from a previous day so its
# stop-loss / re-entry / next-day exit are under script control before the 09:15 open. The regular 10:43 / 10:58
# launches (register_tasks.ps1) are unchanged and start the day's own new cycles.
#   .\scheduler\register_carryover.ps1           # register
#   .\scheduler\register_carryover.ps1 -Remove   # unregister
param([switch]$Remove)
$taskName = "NiftyStrangle-Carryover"
if ($Remove) { Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue; Write-Host "Removed $taskName"; return }
$repo = Split-Path -Parent $PSScriptRoot
$python = (Get-Command python -ErrorAction Stop).Source
$action = New-ScheduledTaskAction -Execute $python -Argument "scheduler\carryover.py" -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday -At "09:13"
# No time limit (it waits for the carried cycles to finish); StartWhenAvailable runs it late if the PC booted after 09:13.
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
Write-Host "Registered $taskName at 09:13 Mon-Fri"
