param(
    [Parameter(Mandatory=$true)]
    [string]$CaseId,

    [string]$WorkspaceRoot = (Get-Location).Path,
    [string]$ModelsConfig,
    [string]$OutputJson,
    [string]$PythonExe = "python"
)

$resolvedWorkspaceRoot = (Resolve-Path -LiteralPath $WorkspaceRoot).Path
$argsList = @("run-case", "--case-id", $CaseId, "--workspace-root", $resolvedWorkspaceRoot)
if ($ModelsConfig) {
    $argsList += @("--models-config", (Resolve-Path -LiteralPath $ModelsConfig).Path)
}
if ($OutputJson) {
    $argsList += @("--output-json", $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputJson))
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location (Join-Path $repoRoot "src")
try {
    & $PythonExe -m graph_pipeline.cli @argsList
}
finally {
    Pop-Location
}
