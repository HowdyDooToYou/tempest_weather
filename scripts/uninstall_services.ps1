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
$briefService = "TempestWeatherDailyBrief"
$emailService = "TempestWeatherDailyEmail"

& $nssm stop $uiService
& $nssm stop $alertService
& $nssm stop $briefService
& $nssm stop $emailService

& $nssm remove $uiService confirm
& $nssm remove $alertService confirm
& $nssm remove $briefService confirm
& $nssm remove $emailService confirm

Write-Host "Services removed: $uiService, $alertService, $briefService, $emailService"
