param(
    [string]$NssmPath = "nssm.exe",
    [string]$RepoPath = "",
    [string]$PythonExe = "",
    [int]$Port = 8501
)

if (-not $RepoPath) {
    $RepoPath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

$nssm = $NssmPath
if (-not (Test-Path $nssm)) {
    $cmd = Get-Command $nssm -ErrorAction SilentlyContinue
    if (-not $cmd) {
        Write-Error "nssm.exe not found. Install NSSM or pass -NssmPath."
        exit 1
    }
    $nssm = $cmd.Source
}

if (-not $PythonExe) {
    $venvPython = Join-Path $RepoPath ".venv\\Scripts\\python.exe"
    if (Test-Path $venvPython) {
        $PythonExe = $venvPython
    } else {
        $PythonExe = "python.exe"
    }
}

$pythonCmd = Get-Command $PythonExe -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    Write-Error "Python executable not found. Pass -PythonExe with a full path."
    exit 1
}
$PythonExe = $pythonCmd.Source

$uiService = "TempestWeatherUI"
$alertService = "TempestWeatherAlerts"
$uiArgs = "-m streamlit run dashboard.py --server.headless true --server.port $Port --server.address 0.0.0.0"
$alertArgs = "-m src.alerts_worker"

$additionalEnvNames = @(
    "SMTP_USERNAME",
    "SMTP_PASSWORD",
    "ALERT_EMAIL_FROM",
    "ALERT_EMAIL_TO",
    "VERIZON_SMS_TO",
    "LOCAL_TZ",
    "TEMPEST_DB_PATH",
    "ALERT_WORKER_INTERVAL_SECONDS"
)
$sharedEnvLines = @()
foreach ($envName in $additionalEnvNames) {
    $value = (Get-ChildItem "env:$envName" -ErrorAction SilentlyContinue).Value
    if ($value) {
        $sharedEnvLines += "$envName=$value"
    }
}
$sharedEnvBlock = $sharedEnvLines -join "`n"

$uiOut = Join-Path $RepoPath "logs\\ui_service.log"
$uiErr = Join-Path $RepoPath "logs\\ui_service_error.log"
$alertOut = Join-Path $RepoPath "logs\\alerts_service.log"
$alertErr = Join-Path $RepoPath "logs\\alerts_service_error.log"

& $nssm install $uiService $PythonExe $uiArgs
& $nssm set $uiService Application $PythonExe
& $nssm set $uiService AppParameters $uiArgs
& $nssm set $uiService AppDirectory $RepoPath
& $nssm set $uiService AppStdout $uiOut
& $nssm set $uiService AppStderr $uiErr
if ($sharedEnvBlock) {
    $uiEnvBlock = "$sharedEnvBlock`nALERTS_WORKER_ENABLED=1"
} else {
    $uiEnvBlock = "ALERTS_WORKER_ENABLED=1"
}
& $nssm set $uiService AppEnvironmentExtra $uiEnvBlock
& $nssm set $uiService Start SERVICE_AUTO_START

& $nssm install $alertService $PythonExe $alertArgs
& $nssm set $alertService Application $PythonExe
& $nssm set $alertService AppParameters $alertArgs
& $nssm set $alertService AppDirectory $RepoPath
& $nssm set $alertService AppStdout $alertOut
& $nssm set $alertService AppStderr $alertErr
if ($sharedEnvBlock) {
    & $nssm set $alertService AppEnvironmentExtra $sharedEnvBlock
}
& $nssm set $alertService Start SERVICE_AUTO_START

& $nssm start $uiService
& $nssm start $alertService

Write-Host "Services installed and started: $uiService, $alertService"
