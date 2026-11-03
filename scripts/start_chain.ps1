# Launch scripts/run_chain.sh detached from the current session (survives the editor closing).
#   powershell -ExecutionPolicy Bypass -File scripts/start_chain.ps1
$root = Split-Path -Parent $PSScriptRoot
$bash = "C:\Program Files\Git\bin\bash.exe"
if (-not (Test-Path $bash)) { $bash = (Get-Command bash).Source }
$script = (Join-Path $root "scripts\run_chain.sh") -replace "\\", "/"
Start-Process -FilePath $bash -ArgumentList @("-lc", "'$script'") -WorkingDirectory $root -WindowStyle Hidden
Write-Output "chain started; follow $root\evals\results\chain.log"
