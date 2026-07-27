# scripts/load-env.ps1
#
# Loads variables from the repo's .env file into the CURRENT PowerShell session,
# so tools that don't read .env themselves (dbt, snowflake-connector, etc.) can
# still see them via env_var()/os.environ.
#
# Usage (from anywhere inside the repo):
#   . <path-to-repo>\scripts\load-env.ps1
#
# The leading ". " (dot-source) is required - running it normally would only
# set the variables in a throwaway child scope.

$repoRoot = $PSScriptRoot | Split-Path -Parent
$envFile = Join-Path $repoRoot ".env"

if (-not (Test-Path $envFile)) {
    Write-Warning "No .env file found at $envFile"
    return
}

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#") -or $line -notmatch "=") { return }

    $parts = $line -split "=", 2
    $name = $parts[0].Trim()
    $value = $parts[1].Trim()
    if ($name) { Set-Item -Path "Env:$name" -Value $value }
}

Write-Host "Loaded environment variables from $envFile" -ForegroundColor Green
