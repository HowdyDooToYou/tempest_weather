param(
    [ValidateSet("status", "start", "stop", "restart", "install", "uninstall", "logs", "env")]
    [string]$Action = "status",
    [ValidateSet("all", "ui", "alerts", "brief", "email")]
    [string]$Target = "all",
    [string]$NssmPath = "nssm.exe",
    [int]$LogLines = 120
)

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$nssm = $NssmPath
if (-not (Test-Path $nssm)) {
    $cmd = Get-Command $nssm -ErrorAction SilentlyContinue
    if ($cmd) {
        $nssm = $cmd.Source
    }
}
if (-not (Test-Path $nssm)) {
    $known = Join-Path $repoRoot "nssm-2.24\\nssm-2.24\\win64\\nssm.exe"
    if (Test-Path $known) {
        $nssm = $known
    }
}
if (-not (Test-Path $nssm)) {
    $found = Get-ChildItem -Path $repoRoot -Recurse -Filter "nssm.exe" -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($found) {
        $nssm = $found.FullName
    }
}
if (-not (Test-Path $nssm)) {
    Write-Error "nssm.exe not found. Install NSSM or pass -NssmPath."
    exit 1
}

$services = @(
    @{ Name = "TempestWeatherUI"; Key = "ui" },
    @{ Name = "TempestWeatherAlerts"; Key = "alerts" },
    @{ Name = "TempestWeatherDailyBrief"; Key = "brief" },
    @{ Name = "TempestWeatherDailyEmail"; Key = "email" }
)

function Get-TargetServices {
    param([string]$target)
    if ($target -eq "all") { return $services }
    return $services | Where-Object { $_.Key -eq $target }
}

function Invoke-Status {
    foreach ($svc in (Get-TargetServices $Target)) {
        & $nssm status $svc.Name
    }
}

function Invoke-Start {
    foreach ($svc in (Get-TargetServices $Target)) {
        & $nssm start $svc.Name
    }
}

function Invoke-Stop {
    foreach ($svc in (Get-TargetServices $Target)) {
        & $nssm stop $svc.Name
    }
}

function Invoke-Restart {
    foreach ($svc in (Get-TargetServices $Target)) {
        & $nssm stop $svc.Name
        Start-Sleep -Seconds 1
        & $nssm start $svc.Name
    }
}

function Invoke-Install {
    $script = Join-Path $PSScriptRoot "install_services.ps1"
    & $script -NssmPath $nssm
}

function Invoke-Uninstall {
    $script = Join-Path $PSScriptRoot "uninstall_services.ps1"
    & $script -NssmPath $nssm
}

function Show-Log {
    param([string]$Path, [string]$Title)
    if (Test-Path $Path) {
        Write-Output ""
        Write-Output "$Title ($Path)"
        Get-Content $Path -Tail $LogLines
    }
}

function Invoke-Logs {
    if ($Target -eq "all" -or $Target -eq "ui") {
        Show-Log (Join-Path $repoRoot "logs\\ui_service.log") "UI service log"
        Show-Log (Join-Path $repoRoot "logs\\ui_service_error.log") "UI service error"
    }
    if ($Target -eq "all" -or $Target -eq "alerts") {
        Show-Log (Join-Path $repoRoot "logs\\alerts_worker.log") "Alerts worker log"
        Show-Log (Join-Path $repoRoot "logs\\alerts_service_error.log") "Alerts service error"
    }
    if ($Target -eq "all" -or $Target -eq "brief") {
        Show-Log (Join-Path $repoRoot "logs\\daily_brief_service.log") "Daily brief log"
        Show-Log (Join-Path $repoRoot "logs\\daily_brief_service_error.log") "Daily brief error"
    }
    if ($Target -eq "all" -or $Target -eq "email") {
        Show-Log (Join-Path $repoRoot "logs\\daily_email_service.log") "Daily email log"
        Show-Log (Join-Path $repoRoot "logs\\daily_email_service_error.log") "Daily email error"
    }
}

function Get-EnvNames {
    param([string]$ServiceName)
    $raw = & $nssm get $ServiceName AppEnvironmentExtra 2>$null
    if ($LASTEXITCODE -ne 0) {
        return @()
    }
    $clean = $raw -replace "`0", ""
    $names = $clean -split "`r?`n" |
        ForEach-Object { ($_ -split "=", 2)[0] } |
        Where-Object { $_ }
    return $names
}

function Invoke-EnvReport {
    $required = @("SMTP_USERNAME", "SMTP_PASSWORD", "ALERT_EMAIL_FROM")
    $transport = @("SMTP_HOST", "SMTP_PORT", "SMTP_USE_TLS", "SMTP_USE_SSL")
    $recipients = @("ALERT_EMAIL_TO", "VERIZON_SMS_TO")
    $alerts = @("FREEZE_WARNING_F", "DEEP_FREEZE_F", "FREEZE_RESET_F")
    $optional = @(
        "LOCAL_TZ",
        "TEMPEST_DB_PATH",
        "TEMPEST_API_TOKEN",
        "ALERT_WORKER_INTERVAL_SECONDS",
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
        "AQI_SMOKE_CLEAR_HOURS",
        "AQI_SMOKE_CLEAR_MAX",
        "AQI_SMOKE_CLEAR_MIN_COUNT"
    )
    foreach ($svc in (Get-TargetServices $Target)) {
        $names = Get-EnvNames $svc.Name
        if (-not $names.Count) {
            Write-Output "$($svc.Name): no AppEnvironmentExtra configured."
            continue
        }
        $usingCredManager = $names -contains "SMTP_CRED_TARGET"
        $missingRequired = $required | Where-Object { $names -notcontains $_ }
        if ($usingCredManager) {
            $missingRequired = $missingRequired | Where-Object { $_ -notin @("SMTP_USERNAME", "SMTP_PASSWORD") }
        }
        $missingTransport = $transport | Where-Object { $names -notcontains $_ }
        $missingRecipients = $recipients | Where-Object { $names -notcontains $_ }
        $missingAlerts = $alerts | Where-Object { $names -notcontains $_ }
        $missingOptional = $optional | Where-Object { $names -notcontains $_ }
        if ($missingRequired) {
            Write-Output "$($svc.Name): missing required: $($missingRequired -join ', ')"
        } else {
            Write-Output "$($svc.Name): required OK"
        }
        if ($missingTransport) {
            Write-Output "$($svc.Name): transport missing: $($missingTransport -join ', ')"
        } else {
            Write-Output "$($svc.Name): transport OK"
        }
        if ($missingRecipients) {
            Write-Output "$($svc.Name): recipients missing: $($missingRecipients -join ', ')"
        }
        if ($missingAlerts) {
            Write-Output "$($svc.Name): alerts missing: $($missingAlerts -join ', ')"
        }
        if ($missingOptional) {
            Write-Output "$($svc.Name): optional missing: $($missingOptional -join ', ')"
        }
        if ($svc.Key -eq "ui" -and $names -notcontains "ALERTS_WORKER_ENABLED") {
            Write-Output "$($svc.Name): ALERTS_WORKER_ENABLED missing"
        }
    }
}

switch ($Action) {
    "status" { Invoke-Status }
    "start" { Invoke-Start }
    "stop" { Invoke-Stop }
    "restart" { Invoke-Restart }
    "install" { Invoke-Install }
    "uninstall" { Invoke-Uninstall }
    "logs" { Invoke-Logs }
    "env" { Invoke-EnvReport }
}
