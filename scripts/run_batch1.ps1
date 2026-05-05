param(
    [Parameter(Mandatory=$true)]
    [string[]]$CaseId,

    [Parameter(Mandatory=$true)]
    [string]$OutputJson,

    [string]$WorkspaceRoot = (Get-Location).Path,
    [string]$ModelsConfig,
    [string]$PythonExe = "python"
)

$resolvedWorkspaceRoot = (Resolve-Path -LiteralPath $WorkspaceRoot).Path
$resolvedOutputJson = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputJson)
$argsList = @("run-batch", "--workspace-root", $resolvedWorkspaceRoot, "--output-json", $resolvedOutputJson)
foreach ($id in $CaseId) {
    $argsList += @("--case-id", $id)
}
if ($ModelsConfig) {
    $argsList += @("--models-config", (Resolve-Path -LiteralPath $ModelsConfig).Path)
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location (Join-Path $repoRoot "src")
try {
    & $PythonExe -m graph_pipeline.cli @argsList
}
finally {
    Pop-Location
}
