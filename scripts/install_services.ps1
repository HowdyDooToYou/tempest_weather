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

if (-not (Get-Command $PythonExe -ErrorAction SilentlyContinue)) {
    Write-Error "Python executable not found. Pass -PythonExe with a full path."
    exit 1
}

$uiService = "TempestWeatherUI"
$alertService = "TempestWeatherAlerts"
$uiArgs = "-m streamlit run dashboard.py --server.headless true --server.port $Port --server.address 0.0.0.0"
$alertArgs = "-m src.alerts_worker"

$uiOut = Join-Path $RepoPath "logs\\ui_service.log"
$uiErr = Join-Path $RepoPath "logs\\ui_service_error.log"
$alertOut = Join-Path $RepoPath "logs\\alerts_service.log"
$alertErr = Join-Path $RepoPath "logs\\alerts_service_error.log"

& $nssm install $uiService $PythonExe $uiArgs
& $nssm set $uiService AppDirectory $RepoPath
& $nssm set $uiService AppStdout $uiOut
& $nssm set $uiService AppStderr $uiErr
& $nssm set $uiService AppEnvironmentExtra "ALERTS_WORKER_ENABLED=1"
& $nssm set $uiService Start SERVICE_AUTO_START

& $nssm install $alertService $PythonExe $alertArgs
& $nssm set $alertService AppDirectory $RepoPath
& $nssm set $alertService AppStdout $alertOut
& $nssm set $alertService AppStderr $alertErr
& $nssm set $alertService Start SERVICE_AUTO_START

& $nssm start $uiService
& $nssm start $alertService

Write-Host "Services installed and started: $uiService, $alertService"
