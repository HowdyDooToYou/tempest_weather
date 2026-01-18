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
$briefService = "TempestWeatherDailyBrief"
$emailService = "TempestWeatherDailyEmail"
$uiArgs = "-m streamlit run dashboard.py --server.headless true --server.port $Port --server.address 0.0.0.0"
$alertArgs = "-m src.alerts_worker"
$briefArgs = "-m src.daily_brief_worker"
$emailArgs = "-m src.daily_email_worker"

$additionalEnvNames = @(
    "SMTP_USERNAME",
    "SMTP_PASSWORD",
    "SMTP_CRED_TARGET",
    "ALERT_EMAIL_FROM",
    "SMTP_HOST",
    "SMTP_PORT",
    "SMTP_USE_TLS",
    "SMTP_USE_SSL",
    "ALERT_EMAIL_TO",
    "VERIZON_SMS_TO",
    "FREEZE_WARNING_F",
    "DEEP_FREEZE_F",
    "FREEZE_RESET_F",
    "LOCAL_TZ",
    "TEMPEST_DB_PATH",
    "TEMPEST_API_TOKEN",
    "TEMPEST_API_KEY",
    "ALERT_WORKER_INTERVAL_SECONDS",
    "DAILY_BRIEF_INTERVAL_MINUTES",
    "DAILY_BRIEF_MODEL",
    "OPENAI_API_KEY",
    "DAILY_BRIEF_LAT",
    "DAILY_BRIEF_LON",
    "NWS_USER_AGENT",
    "NWS_ALERTS_ENABLED",
    "NWS_HWO_NOTIFY",
    "NWS_ZONE",
    "DAILY_EMAIL_TO",
    "DAILY_EMAIL_HOUR",
    "DAILY_EMAIL_MINUTE",
    "DAILY_EMAIL_LAT",
    "DAILY_EMAIL_LON",
    "TEMPEST_STATION_ID"
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
$briefOut = Join-Path $RepoPath "logs\\daily_brief_service.log"
$briefErr = Join-Path $RepoPath "logs\\daily_brief_service_error.log"
$emailOut = Join-Path $RepoPath "logs\\daily_email_service.log"
$emailErr = Join-Path $RepoPath "logs\\daily_email_service_error.log"

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

& $nssm install $briefService $PythonExe $briefArgs
& $nssm set $briefService Application $PythonExe
& $nssm set $briefService AppParameters $briefArgs
& $nssm set $briefService AppDirectory $RepoPath
& $nssm set $briefService AppStdout $briefOut
& $nssm set $briefService AppStderr $briefErr
if ($sharedEnvBlock) {
    & $nssm set $briefService AppEnvironmentExtra $sharedEnvBlock
}
& $nssm set $briefService Start SERVICE_AUTO_START

& $nssm install $emailService $PythonExe $emailArgs
& $nssm set $emailService Application $PythonExe
& $nssm set $emailService AppParameters $emailArgs
& $nssm set $emailService AppDirectory $RepoPath
& $nssm set $emailService AppStdout $emailOut
& $nssm set $emailService AppStderr $emailErr
if ($sharedEnvBlock) {
    & $nssm set $emailService AppEnvironmentExtra $sharedEnvBlock
}
& $nssm set $emailService Start SERVICE_AUTO_START

& $nssm start $uiService
& $nssm start $alertService
& $nssm start $briefService
& $nssm start $emailService

Write-Host "Services installed and started: $uiService, $alertService, $briefService, $emailService"
