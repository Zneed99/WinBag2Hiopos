# Installs (or updates) WinBag2Hiopos as a Windows service with NSSM.
# Started by install.bat in this folder - run that one instead.
#
# STANDARD LAYOUT (the same on every installation):
#   C:\ICG\WinBag2Hiopos\
#       App\        WinBag2HioposWatcher.exe, nssm.exe
#       Logs\       install.log, service_output.log
#   Windows service: ICG-WinBag2Hiopos  (shown as "ICG - WinBag2Hiopos")
#
# The program's own data folder (C:\winbag_export - Input_Files_Here,
# Imported_Files, Old Files, PCS_Archive, Logs) is separate and unaffected
# by this installer; it is where WinBag itself reads/writes files.

$Root        = "C:\ICG\WinBag2Hiopos"
$AppDir      = Join-Path $Root "App"
$LogDir      = Join-Path $Root "Logs"
$ServiceName = "ICG-WinBag2Hiopos"
$DisplayName = "ICG - WinBag2Hiopos"
$DataFolder  = "C:\winbag_export"

$ErrorActionPreference = "Stop"
$projectDir = Split-Path $PSScriptRoot -Parent

function Step($text) { Write-Host ""; Write-Host "==> $text" -ForegroundColor Cyan }
function Fail($text) {
    Write-Host ""
    Write-Host "ERROR: $text" -ForegroundColor Red
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}

function Nssm([string[]]$nssmArgs) {
    $out = & $script:nssm @nssmArgs 2>&1 | Out-String
    $out = ($out -replace "`0", "").Trim()
    if ($LASTEXITCODE -ne 0) { Fail "nssm $($nssmArgs -join ' ') failed: $out" }
}

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

# --- Checks ------------------------------------------------------------
$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "ERROR: Must be run as administrator. Double-click install.bat instead." -ForegroundColor Red
    exit 1
}

New-Item -ItemType Directory -Force -Path $AppDir, $LogDir | Out-Null
Start-Transcript -Path (Join-Path $LogDir "install.log") -Append | Out-Null
Write-Host "Installing from: $projectDir"

$exeSource  = Join-Path $projectDir "dist\WinBag2HioposWatcher.exe"
$nssmSource = Join-Path $PSScriptRoot "nssm.exe"
foreach ($file in @($exeSource, $nssmSource)) {
    if (-not (Test-Path $file)) { Fail "Missing file: $file" }
}

# --- Remove an existing service (update) --------------------------------
$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existing) {
    Step "Existing service '$ServiceName' found - replacing it"
    Stop-ServiceSafely $ServiceName
    & sc.exe delete $ServiceName | Out-Null
    for ($i = 0; $i -lt 20 -and (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue); $i++) {
        Start-Sleep -Milliseconds 500
    }
    if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
        Fail "The old service could not be removed. Close the 'Services' window (services.msc) if it is open, and run install.bat again."
    }
}
# A leftover program process would lock the .exe we are about to replace.
# Only touch the copy in our own App folder.
Get-Process -Name "WinBag2HioposWatcher" -ErrorAction SilentlyContinue |
    Where-Object { $_.Path -like "$AppDir\*" } | Stop-Process -Force
Start-Sleep -Seconds 1

# --- Copy files ------------------------------------------------------------
Step "Copying program files to $AppDir"
Copy-Item $exeSource  (Join-Path $AppDir "WinBag2HioposWatcher.exe") -Force
Copy-Item $nssmSource (Join-Path $AppDir "nssm.exe") -Force

# Files from a downloaded ZIP are marked as 'from the internet'; clear that.
Get-ChildItem $AppDir -File | Unblock-File

# --- Install and start the service -----------------------------------------
$script:nssm = Join-Path $AppDir "nssm.exe"
$serviceOutput = Join-Path $LogDir "service_output.log"
Step "Installing service '$DisplayName'"
Nssm @("install", $ServiceName, (Join-Path $AppDir "WinBag2HioposWatcher.exe"))
Nssm @("set", $ServiceName, "AppDirectory", $AppDir)
Nssm @("set", $ServiceName, "DisplayName", $DisplayName)
Nssm @("set", $ServiceName, "Description", "Watches $DataFolder for WinBag export/import files and converts them for Hiopos. Logs: $DataFolder\Logs")
Nssm @("set", $ServiceName, "Start", "SERVICE_AUTO_START")
# Crashes / startup errors end up here. The program's own per-file activity
# log is separate - see $DataFolder\Logs\winbag2hiopos.log.
Nssm @("set", $ServiceName, "AppStdout", $serviceOutput)
Nssm @("set", $ServiceName, "AppStderr", $serviceOutput)

Step "Starting service"
Start-Service -Name $ServiceName
(Get-Service -Name $ServiceName).WaitForStatus("Running", [TimeSpan]::FromSeconds(30))
Start-Sleep -Seconds 3
if ((Get-Service -Name $ServiceName).Status -ne "Running") {
    Fail "The service did not keep running. Look in $serviceOutput and $DataFolder\Logs\winbag2hiopos.log"
}

# --- Done ------------------------------------------------------------------
Write-Host ""
Write-Host "=====================================================" -ForegroundColor Green
Write-Host " $DisplayName is installed and running." -ForegroundColor Green
Write-Host "=====================================================" -ForegroundColor Green
Write-Host " Export files land in: $DataFolder\Input_Files_Here"
Write-Host " Drop pcs.adm into:    $DataFolder"
Write-Host " Activity log:         $DataFolder\Logs\winbag2hiopos.log"
Write-Host " Service crash log:    $serviceOutput"
Stop-Transcript | Out-Null
exit 0
