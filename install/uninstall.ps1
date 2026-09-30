# Removes the WinBag2Hiopos Windows service (ICG-WinBag2Hiopos).
# Started by uninstall.bat in this folder - run that one instead.
# The folder C:\ICG\WinBag2Hiopos (program, install log) is NOT deleted, and
# neither is C:\winbag_export (the program's data - Input_Files_Here,
# Imported_Files, Old Files, PCS_Archive, Logs).
param(
    [string]$ServiceName = "ICG-WinBag2Hiopos"
)

$Root   = "C:\ICG\WinBag2Hiopos"
$LogDir = Join-Path $Root "Logs"
$ErrorActionPreference = "Stop"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "ERROR: Must be run as administrator. Double-click uninstall.bat instead." -ForegroundColor Red
    exit 1
}

if (Test-Path $LogDir) { Start-Transcript -Path (Join-Path $LogDir "install.log") -Append | Out-Null }

function Stop-ServiceSafely($name) {
    # Stops the service; if it hangs, its NSSM process is killed instead.
    $svc = Get-CimInstance Win32_Service -Filter "Name='$name'"
    if (-not $svc -or $svc.State -eq "Stopped") { return }
    & sc.exe stop $name | Out-Null
    for ($i = 0; $i -lt 30; $i++) {
        if ((Get-Service -Name $name).Status -eq "Stopped") { return }
        Start-Sleep -Seconds 1
    }
    Write-Host "    Service did not stop within 30 seconds - forcing it."
    $svc = Get-CimInstance Win32_Service -Filter "Name='$name'"
    if ($svc.ProcessId) { Stop-Process -Id $svc.ProcessId -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 2
}

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if (-not $existing) {
    Write-Host "The service '$ServiceName' is not installed - nothing to do."
} else {
    Write-Host "Stopping service '$ServiceName'..."
    Stop-ServiceSafely $ServiceName
    & sc.exe delete $ServiceName | Out-Null
    for ($i = 0; $i -lt 20 -and (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue); $i++) {
        Start-Sleep -Milliseconds 500
    }
    if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
        Write-Host "The service is marked for removal and disappears when the 'Services' window is closed or after a restart." -ForegroundColor Yellow
    } else {
        Write-Host "Service '$ServiceName' removed." -ForegroundColor Green
    }
    Write-Host "The folder $Root (program, install log) was kept."
    Write-Host "The data folder C:\winbag_export (Input, Imported_Files, Logs etc.) was kept."
    Write-Host "Delete either by hand if they are no longer needed."
}

if (Test-Path $LogDir) { Stop-Transcript | Out-Null }
exit 0
