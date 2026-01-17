param(
    [string]$NssmPath = "nssm.exe"
)

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$servicesScript = Join-Path $repoRoot "scripts\\services.ps1"
if (-not (Test-Path $servicesScript)) {
    Write-Error "Missing scripts/services.ps1. Run from the repo root or reinstall scripts."
    exit 1
}

function Show-Menu {
    Write-Output ""
    Write-Output "Tempest Weather Services"
    Write-Output "1) Status (all)"
    Write-Output "2) Start (all)"
    Write-Output "3) Stop (all)"
    Write-Output "4) Restart (all)"
    Write-Output "5) Start UI"
    Write-Output "6) Start Alerts"
    Write-Output "7) Start Daily Brief"
    Write-Output "8) Start Daily Email"
    Write-Output "9) Logs (UI)"
    Write-Output "A) Logs (Alerts)"
    Write-Output "B) Logs (Daily Brief)"
    Write-Output "C) Logs (Daily Email)"
    Write-Output "D) Env Report (keys only)"
    Write-Output "I) Install services"
    Write-Output "U) Uninstall services"
    Write-Output "0) Exit"
}

function Read-LogLines {
    param([int]$DefaultLines)
    $input = Read-Host "Log lines (default $DefaultLines)"
    if ([string]::IsNullOrWhiteSpace($input)) {
        return $DefaultLines
    }
    $parsed = 0
    if ([int]::TryParse($input, [ref]$parsed) -and $parsed -gt 0) {
        return $parsed
    }
    Write-Output "Invalid number, using default $DefaultLines."
    return $DefaultLines
}

$defaultLogLines = 120

while ($true) {
    Show-Menu
    $choice = (Read-Host "Select option").Trim().ToLower()
    switch ($choice) {
        "1" { & $servicesScript -Action status -Target all -NssmPath $NssmPath }
        "2" { & $servicesScript -Action start -Target all -NssmPath $NssmPath }
        "3" { & $servicesScript -Action stop -Target all -NssmPath $NssmPath }
        "4" { & $servicesScript -Action restart -Target all -NssmPath $NssmPath }
        "5" { & $servicesScript -Action start -Target ui -NssmPath $NssmPath }
        "6" { & $servicesScript -Action start -Target alerts -NssmPath $NssmPath }
        "7" { & $servicesScript -Action start -Target brief -NssmPath $NssmPath }
        "8" { & $servicesScript -Action start -Target email -NssmPath $NssmPath }
        "9" {
            $lines = Read-LogLines -DefaultLines $defaultLogLines
            & $servicesScript -Action logs -Target ui -LogLines $lines -NssmPath $NssmPath
        }
        "a" {
            $lines = Read-LogLines -DefaultLines $defaultLogLines
            & $servicesScript -Action logs -Target alerts -LogLines $lines -NssmPath $NssmPath
        }
        "b" {
            $lines = Read-LogLines -DefaultLines $defaultLogLines
            & $servicesScript -Action logs -Target brief -LogLines $lines -NssmPath $NssmPath
        }
        "c" {
            $lines = Read-LogLines -DefaultLines $defaultLogLines
            & $servicesScript -Action logs -Target email -LogLines $lines -NssmPath $NssmPath
        }
        "d" { & $servicesScript -Action env -Target all -NssmPath $NssmPath }
        "i" { & $servicesScript -Action install -Target all -NssmPath $NssmPath }
        "u" { & $servicesScript -Action uninstall -Target all -NssmPath $NssmPath }
        "0" { break }
        "q" { break }
        default { Write-Output "Unknown option. Try again." }
    }
}
