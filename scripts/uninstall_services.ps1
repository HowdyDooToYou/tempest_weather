param(
    [string]$NssmPath = "nssm.exe"
)

$nssm = $NssmPath
if (-not (Test-Path $nssm)) {
    $cmd = Get-Command $nssm -ErrorAction SilentlyContinue
    if (-not $cmd) {
        Write-Error "nssm.exe not found. Install NSSM or pass -NssmPath."
        exit 1
    }
    $nssm = $cmd.Source
}

$uiService = "TempestWeatherUI"
$alertService = "TempestWeatherAlerts"

& $nssm stop $uiService
& $nssm stop $alertService

& $nssm remove $uiService confirm
& $nssm remove $alertService confirm

Write-Host "Services removed: $uiService, $alertService"
