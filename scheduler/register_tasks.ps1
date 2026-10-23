# Registers the 4 strategy launchers in Windows Task Scheduler (Mon-Fri, local time,
# so the PC's timezone must be IST). Each launches 2 minutes before the strategy's
# own trigger time, same as the old crontab. Paper mode unless -Live is passed.
#   Register:    .\scheduler\register_tasks.ps1 [-Live]
#   Unregister:  .\scheduler\register_tasks.ps1 -Remove
param(
    [switch]$Live,
    [switch]$Remove
)

$runner = Join-Path $PSScriptRoot "run_strategy.ps1"

$jobs = @(
    @{ Name = "Nifty-Strangle";     At = "10:58"; Script = "Nifty\strategy\delta_strangle_strategy.py";      Log = "nifty_strangle" },
    @{ Name = "Sensex-Strangle";    At = "10:58"; Script = "Sensex\strategy\delta_strangle_strategy.py";     Log = "sensex_strangle" },
    @{ Name = "Nifty-Breakdown";    At = "10:43"; Script = "Nifty\strategy\delta_breakdown_strategy.py";     Log = "nifty_breakdown" },
    @{ Name = "Sensex-Breakdown";   At = "10:43"; Script = "Sensex\strategy\delta_breakdown_strategy.py";    Log = "sensex_breakdown" }
)

foreach ($j in $jobs) {
    $taskName = "NiftyStrangle-$($j.Name)"
    if ($Remove) {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
        Write-Host "Removed $taskName"
        continue
    }

    $liveArg = if ($Live) { " -Live" } else { "" }
    $arg = "-NoProfile -ExecutionPolicy Bypass -File `"$runner`" -Script `"$($j.Script)`" -LogName $($j.Log)$liveArg"
    $action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $arg
    $trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday -At $j.At
    # Strategies run overnight, so no time limit. Parallel: a breakdown cycle carries into the next day
    # while the next day's 10:45 cycle starts as its own process (each uses its own dated state file).
    $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances Parallel `
        -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
    Write-Host "Registered $taskName at $($j.At) Mon-Fri$(if ($Live) { ' [LIVE]' } else { ' [paper]' })"
}
