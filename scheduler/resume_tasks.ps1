# Re-attach to positions left open by an outage: runs each strategy against a specific saved state file
# (resume_*_state.json) as its own live process, separate from the day's fresh scheduled cycles.
#   .\scheduler\resume_tasks.ps1          # register and start
#   .\scheduler\resume_tasks.ps1 -Remove  # unregister
param([switch]$Remove)
$runner = Join-Path $PSScriptRoot "run_strategy.ps1"
$jobs = @(
    @{ Name = "Nifty-Strangle";     Script = "Nifty\strategy\delta_strangle_strategy.py";     State = "resume_nifty_strangle_state.json";     Log = "resume_nifty_strangle" },
    @{ Name = "Sensex-Strangle";    Script = "Sensex\strategy\delta_strangle_strategy.py";    State = "resume_sensex_strangle_state.json";    Log = "resume_sensex_strangle" },
    @{ Name = "Nifty-Breakdown";    Script = "Nifty\strategy\delta_breakdown_strategy.py";    State = "resume_nifty_breakdown_state.json";    Log = "resume_nifty_breakdown" },
    @{ Name = "Sensex-Breakdown";   Script = "Sensex\strategy\delta_breakdown_strategy.py";   State = "resume_sensex_breakdown_state.json";   Log = "resume_sensex_breakdown" }
)
foreach ($j in $jobs) {
    $taskName = "NiftyStrangle-Resume-$($j.Name)"
    if ($Remove) { Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue; Write-Host "Removed $taskName"; continue }
    $arg = "-NoProfile -ExecutionPolicy Bypass -File `"$runner`" -Script `"$($j.Script)`" -LogName $($j.Log) -StateFile `"$($j.State)`" -Live"
    $action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $arg
    $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    Register-ScheduledTask -TaskName $taskName -Action $action -Settings $settings -Force | Out-Null
    Start-ScheduledTask -TaskName $taskName
    Write-Host "Started $taskName"
}
