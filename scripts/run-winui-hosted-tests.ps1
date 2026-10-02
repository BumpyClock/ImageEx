#Requires -Version 7.0
<#
.SYNOPSIS
Builds and runs the packaged ImageEx WinUI hosted tests.

.DESCRIPTION
Builds tests/ImageEx.Hosted.Tests for x64, runs its .build.appxrecipe through
Visual Studio's vstest.console.exe, and publishes results under
artifacts/test-results. The latest result is artifacts/test-results/imageex-hosted.trx.

The script fails when the TRX is missing or incomplete, when it reports no
results, when any result is not Passed, or when a test method in the packaged
assembly has no result.
#>
[CmdletBinding()]
param(
    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Debug',

    [string]$ResultsDirectory,

    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'

$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$projectRoot = Join-Path $repoRoot 'tests\ImageEx.Hosted.Tests'
$projectPath = Join-Path $projectRoot 'ImageEx.Hosted.Tests.csproj'
$assemblyFileName = 'ImageEx.Hosted.Tests.dll'
$lockFilePath = Join-Path $repoRoot 'ImageEx\packages.lock.json'
$resultsRoot = if ([string]::IsNullOrWhiteSpace($ResultsDirectory)) {
    Join-Path $repoRoot 'artifacts\test-results'
} else {
    [System.IO.Path]::GetFullPath($ResultsDirectory)
}
$buildLogsRoot = Join-Path $repoRoot 'artifacts\build-logs'
$stableTrxName = 'imageex-hosted.trx'
$stableTrxPath = Join-Path $resultsRoot $stableTrxName

function Get-TestAssemblyPath {
    param([string]$RecipePath)

    [xml]$recipe = [System.IO.File]::ReadAllText($RecipePath)
    $entries = @($recipe.SelectNodes(
        "//*[local-name()='AppxPackagedFile'][*[local-name()='PackagePath']='$assemblyFileName']"))
    if ($entries.Count -ne 1) {
        throw "Expected one packaged $assemblyFileName in $RecipePath, found $($entries.Count)."
    }

    $path = $entries[0].GetAttribute('Include')
    if (-not [System.IO.File]::Exists($path)) {
        throw "Packaged test assembly is missing: $path"
    }

    return $path
}

function Get-AttributeTypeName {
    param($Metadata, $Attribute)

    $constructor = $Attribute.Constructor
    if ($constructor.Kind -eq [System.Reflection.Metadata.HandleKind]::MemberReference) {
        $parent = $Metadata.GetMemberReference([System.Reflection.Metadata.MemberReferenceHandle]$constructor).Parent
        if ($parent.Kind -ne [System.Reflection.Metadata.HandleKind]::TypeReference) { return $null }
        $type = $Metadata.GetTypeReference([System.Reflection.Metadata.TypeReferenceHandle]$parent)
        return "$($Metadata.GetString($type.Namespace)).$($Metadata.GetString($type.Name))"
    }
    if ($constructor.Kind -eq [System.Reflection.Metadata.HandleKind]::MethodDefinition) {
        $method = $Metadata.GetMethodDefinition([System.Reflection.Metadata.MethodDefinitionHandle]$constructor)
        $type = $Metadata.GetTypeDefinition($method.GetDeclaringType())
        return "$($Metadata.GetString($type.Namespace)).$($Metadata.GetString($type.Name))"
    }

    return $null
}

function Test-HasAttribute {
    param($Metadata, $Handles, [string[]]$TypeNames)

    foreach ($handle in $Handles) {
        if ((Get-AttributeTypeName $Metadata $Metadata.GetCustomAttribute($handle)) -in $TypeNames) {
            return $true
        }
    }

    return $false
}

# Reads test methods from assembly metadata so completeness does not depend on vstest discovery.
function Get-DiscoveredTestNames {
    param([string]$AssemblyPath)

    $classAttributes = @('Microsoft.VisualStudio.TestTools.UnitTesting.TestClassAttribute')
    $methodAttributes = @(
        'Microsoft.VisualStudio.TestTools.UnitTesting.TestMethodAttribute'
        'Microsoft.VisualStudio.TestTools.UnitTesting.DataTestMethodAttribute'
        'Microsoft.VisualStudio.TestTools.UnitTesting.AppContainer.UITestMethodAttribute'
    )
    $stream = [System.IO.File]::OpenRead($AssemblyPath)
    $peReader = [System.Reflection.PortableExecutable.PEReader]::new($stream)
    try {
        $metadata = [System.Reflection.Metadata.PEReaderExtensions]::GetMetadataReader($peReader)
        foreach ($typeHandle in $metadata.TypeDefinitions) {
            $type = $metadata.GetTypeDefinition($typeHandle)
            if (-not (Test-HasAttribute $metadata $type.GetCustomAttributes() $classAttributes)) { continue }

            $typeName = "$($metadata.GetString($type.Namespace)).$($metadata.GetString($type.Name))"
            foreach ($methodHandle in $type.GetMethods()) {
                $method = $metadata.GetMethodDefinition($methodHandle)
                if (Test-HasAttribute $metadata $method.GetCustomAttributes() $methodAttributes) {
                    "$typeName.$($metadata.GetString($method.Name))"
                }
            }
        }
    }
    finally {
        $peReader.Dispose()
        $stream.Dispose()
    }
}

function Invoke-Native {
    param([scriptblock]$Command)

    $previous = $PSNativeCommandUseErrorActionPreference
    try {
        $PSNativeCommandUseErrorActionPreference = $false
        & $Command
        return $LASTEXITCODE
    }
    finally {
        $PSNativeCommandUseErrorActionPreference = $previous
    }
}

if (-not [System.IO.File]::Exists($projectPath)) {
    throw "Hosted test project not found: $projectPath"
}

[System.IO.Directory]::CreateDirectory($resultsRoot) | Out-Null
[System.IO.Directory]::CreateDirectory($buildLogsRoot) | Out-Null
[System.IO.File]::Delete($stableTrxPath)

if (-not $SkipBuild) {
    # The SDK can rewrite the library lockfile during restore. Keep the committed file unchanged.
    $lockFileSnapshot = [System.IO.File]::ReadAllBytes($lockFilePath)
    try {
        $buildExitCode = Invoke-Native {
            dotnet build $projectPath `
                -c $Configuration `
                '-p:Platform=x64' `
                -r win-x64 `
                '-p:AppxPackageSigningEnabled=false' `
                "-bl:$(Join-Path $buildLogsRoot 'imageex-hosted.binlog')" `
                --disable-build-servers | Out-Host
        }
    }
    finally {
        [System.IO.File]::WriteAllBytes($lockFilePath, $lockFileSnapshot)
    }

    if ($buildExitCode -ne 0) {
        throw "Hosted test build failed with exit code $buildExitCode."
    }
}

$recipeRoot = Join-Path $projectRoot "bin\x64\$Configuration"
if (-not [System.IO.Directory]::Exists($recipeRoot)) {
    throw "Hosted test output directory not found: $recipeRoot"
}

$recipes = @([System.IO.Directory]::GetFiles($recipeRoot, '*.build.appxrecipe', [System.IO.SearchOption]::AllDirectories))
if ($recipes.Count -ne 1) {
    throw "Expected one .build.appxrecipe under $recipeRoot, found $($recipes.Count)."
}

$discoveredNames = [System.Collections.Generic.HashSet[string]]::new(
    [string[]]@(Get-DiscoveredTestNames -AssemblyPath (Get-TestAssemblyPath -RecipePath $recipes[0])),
    [StringComparer]::Ordinal)
if ($discoveredNames.Count -eq 0) {
    throw 'The packaged test assembly contains no test methods.'
}

$vswherePath = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
if (-not [System.IO.File]::Exists($vswherePath)) {
    $vswherePath = Get-Command vswhere.exe -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty Source
}
if (-not $vswherePath) {
    throw 'vswhere.exe was not found. Install Visual Studio with packaged test support.'
}

$installationPaths = @(& $vswherePath -latest -products * -property installationPath | Where-Object { $_ })
if ($installationPaths.Count -ne 1) {
    throw "Expected one latest Visual Studio installation from vswhere, found $($installationPaths.Count)."
}

$vstestPath = Join-Path $installationPaths[0] 'Common7\IDE\CommonExtensions\Microsoft\TestWindow\vstest.console.exe'
if (-not [System.IO.File]::Exists($vstestPath)) {
    throw "Visual Studio vstest.console.exe not found: $vstestPath"
}

# A short staging root keeps result attachment paths below MAX_PATH.
$staging = [System.IO.Directory]::CreateTempSubdirectory('ixh-')
$runTrxName = "imageex-hosted-$([Guid]::NewGuid().ToString('N')).trx"
try {
    $testExitCode = Invoke-Native {
        & $vstestPath $recipes[0] '/Platform:x64' "/Logger:trx;LogFileName=$runTrxName" `
            "/ResultsDirectory:$($staging.FullName)" "/Diag:$(Join-Path $staging.FullName 'vstest.diag.log')" | Out-Host
    }
}
finally {
    # Preserve raw results and diagnostics even when the run fails.
    $published = Join-Path $resultsRoot ([System.IO.Path]::GetFileNameWithoutExtension($runTrxName))
    Copy-Item -LiteralPath $staging.FullName -Destination $published -Recurse
    $runTrxPath = Join-Path $published $runTrxName
    if ([System.IO.File]::Exists($runTrxPath)) {
        $temporaryTrx = "$stableTrxPath.$([Guid]::NewGuid().ToString('N')).tmp"
        Copy-Item -LiteralPath $runTrxPath -Destination $temporaryTrx
        [System.IO.File]::Move($temporaryTrx, $stableTrxPath, $true)
    }
    $staging.Delete($true)
}

if ($testExitCode -ne 0) {
    throw "vstest.console.exe failed with exit code $testExitCode. Results: $published"
}

if (-not [System.IO.File]::Exists($stableTrxPath)) {
    throw "The hosted run did not produce a TRX: $stableTrxPath"
}

[xml]$trx = [System.IO.File]::ReadAllText($stableTrxPath)
$summaries = @($trx.SelectNodes("/*[local-name()='TestRun']/*[local-name()='ResultSummary']"))
if ($summaries.Count -ne 1 -or $summaries[0].GetAttribute('outcome') -notin @('Completed', 'Passed')) {
    throw 'The TRX does not report a completed run.'
}

$definitions = @{}
foreach ($unitTest in $trx.SelectNodes("/*[local-name()='TestRun']/*[local-name()='TestDefinitions']/*[local-name()='UnitTest']")) {
    $testMethod = $unitTest.SelectSingleNode("*[local-name()='TestMethod']")
    $className = (([string]$testMethod.GetAttribute('className')) -split ',')[0].Trim()
    $definitions[$unitTest.GetAttribute('id')] = "$className.$($testMethod.GetAttribute('name'))"
}

$results = @($trx.SelectNodes("/*[local-name()='TestRun']/*[local-name()='Results']/*[local-name()='UnitTestResult']"))
if ($results.Count -eq 0) {
    throw 'The TRX reports no test results.'
}

$nonPassing = @($results | Where-Object { $_.GetAttribute('outcome') -ne 'Passed' })
if ($nonPassing.Count -ne 0) {
    $summary = ($nonPassing | ForEach-Object { "$($_.GetAttribute('testName')): $($_.GetAttribute('outcome'))" }) -join '; '
    throw "Non-passing hosted results: $summary"
}

$executedNames = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($result in $results) {
    $testId = $result.GetAttribute('testId')
    if (-not $definitions.ContainsKey($testId)) {
        throw "TRX result '$($result.GetAttribute('testName'))' has no test definition."
    }
    [void]$executedNames.Add($definitions[$testId])
}

$missing = @($discoveredNames | Where-Object { -not $executedNames.Contains($_) } | Sort-Object)
if ($missing.Count -ne 0) {
    throw "The TRX has no result for: $($missing -join '; ')"
}
$unexpected = @($executedNames | Where-Object { -not $discoveredNames.Contains($_) } | Sort-Object)
if ($unexpected.Count -ne 0) {
    throw "The TRX reports tests that the packaged assembly does not contain: $($unexpected -join '; ')"
}

Write-Host "Passed: $($results.Count) results cover all $($discoveredNames.Count) hosted tests. TRX: $stableTrxPath"
