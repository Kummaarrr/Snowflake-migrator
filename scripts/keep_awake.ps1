# Keep Windows from sleeping while the measurement chain runs; exits (and lets the machine sleep
# again) once evals/results/chain.done exists. Uses SetThreadExecutionState, which needs no
# admin rights and changes no power settings.
#   powershell -ExecutionPolicy Bypass -WindowStyle Hidden -File scripts/keep_awake.ps1
$root = Split-Path -Parent $PSScriptRoot
$done = Join-Path $root "evals\results\chain.done"
Add-Type -Namespace Win32 -Name Power -MemberDefinition @"
[DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint esFlags);
"@
$ES_CONTINUOUS = [uint32]"0x80000000"
$ES_SYSTEM_REQUIRED = [uint32]"0x00000001"
[Win32.Power]::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED) | Out-Null
while (-not (Test-Path $done)) {
    [Win32.Power]::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED) | Out-Null
    Start-Sleep -Seconds 60
}
[Win32.Power]::SetThreadExecutionState($ES_CONTINUOUS) | Out-Null
