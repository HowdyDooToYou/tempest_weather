param(
    [string]$EnvFile = ".env",
    [int]$Port = 8501,
    [switch]$OverrideEnv
)

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$envPath = Join-Path $repoRoot $EnvFile

if (Test-Path $envPath) {
    Get-Content $envPath | ForEach-Object {
        $line = $_.Trim()
        if (-not $line) { return }
        if ($line.StartsWith("#")) { return }
        $pair = $line -split "=", 2
        if ($pair.Count -ne 2) { return }
        $name = $pair[0].Trim()
        $value = $pair[1].Trim()
        if ($value.StartsWith('"') -and $value.EndsWith('"')) {
            $value = $value.Trim('"')
        }
        if ($value.StartsWith("'") -and $value.EndsWith("'")) {
            $value = $value.Trim("'")
        }
        $existing = (Get-ChildItem "Env:$name" -ErrorAction SilentlyContinue).Value
        if ($OverrideEnv -or [string]::IsNullOrWhiteSpace($existing)) {
            Set-Item -Path "Env:$name" -Value $value
        }
    }
}

Set-Location $repoRoot
python -m streamlit run dashboard.py --server.headless true --server.port $Port --server.address 0.0.0.0
