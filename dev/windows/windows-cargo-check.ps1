param(
    [string[]]$CargoArgs = @("check", "-p", "surreal"),
    [switch]$InstallMissingTools,
    [switch]$ShowGeneratedCmd
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-VsDevCmdPath {
    $vswhereCandidates = @(
        "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe",
        "${env:ProgramFiles}\Microsoft Visual Studio\Installer\vswhere.exe"
    )

    foreach ($vswhere in $vswhereCandidates) {
        if (-not (Test-Path -LiteralPath $vswhere -PathType Leaf)) {
            continue
        }

        $installPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
        if (-not [string]::IsNullOrWhiteSpace($installPath)) {
            $candidate = Join-Path $installPath.Trim() "Common7\Tools\VsDevCmd.bat"
            if (Test-Path -LiteralPath $candidate -PathType Leaf) {
                return $candidate
            }
        }
    }

    $fallbacks = @(
        "C:\Program Files\Microsoft Visual Studio\2022\BuildTools\Common7\Tools\VsDevCmd.bat",
        "C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\Tools\VsDevCmd.bat"
    )

    foreach ($fallback in $fallbacks) {
        if (Test-Path -LiteralPath $fallback -PathType Leaf) {
            return $fallback
        }
    }

    return $null
}

function Get-LlvmBinPath {
    $candidates = @(
        (Join-Path $env:USERPROFILE "scoop\apps\llvm\current\bin"),
        "C:\Program Files\LLVM\bin"
    )

    foreach ($candidate in $candidates) {
        if (-not (Test-Path -LiteralPath $candidate -PathType Container)) {
            continue
        }
        if ((Test-Path -LiteralPath (Join-Path $candidate "clang.exe") -PathType Leaf) -and
            (Test-Path -LiteralPath (Join-Path $candidate "libclang.dll") -PathType Leaf)) {
            return $candidate
        }
    }

    return $null
}

function Get-CargoCommand {
    $cargo = Get-Command cargo -ErrorAction SilentlyContinue
    if ($cargo) {
        return $cargo
    }

    $fallbacks = @(
        (Join-Path $env:USERPROFILE ".cargo\bin\cargo.exe"),
        (Join-Path $env:USERPROFILE "scoop\apps\rustup\current\.cargo\bin\cargo.exe"),
        (Join-Path $env:USERPROFILE "scoop\shims\cargo.exe")
    )

    foreach ($fallback in $fallbacks) {
        if (Test-Path -LiteralPath $fallback -PathType Leaf) {
            return Get-Item -LiteralPath $fallback
        }
    }

    return $null
}

function Ensure-ScoopPackage {
    param(
        [string]$PackageName
    )

    $scoop = Get-Command scoop -ErrorAction SilentlyContinue
    if (-not $scoop) {
        $shim = Join-Path $env:USERPROFILE "scoop\shims\scoop.cmd"
        if (Test-Path -LiteralPath $shim -PathType Leaf) {
            $scoop = Get-Item -LiteralPath $shim
        }
    }

    if (-not $scoop) {
        throw "Scoop was not found, cannot auto-install '$PackageName'."
    }

    $scoopCmd = if ($scoop -is [System.IO.FileInfo]) { $scoop.FullName } else { $scoop.Source }

    & $scoopCmd list $PackageName *> $null
    if ($LASTEXITCODE -eq 0) {
        return
    }

    Write-Host "Installing '$PackageName' via Scoop..."
    & $scoopCmd install $PackageName
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install '$PackageName' via Scoop."
    }
}

function Get-NasmCommand {
    $nasm = Get-Command nasm -ErrorAction SilentlyContinue
    if ($nasm) {
        return $nasm
    }

    $nasmShim = Join-Path $env:USERPROFILE "scoop\shims\nasm.exe"
    if (Test-Path -LiteralPath $nasmShim -PathType Leaf) {
        return Get-Item -LiteralPath $nasmShim
    }

    return $null
}

function Normalize-PathValue {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Value,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedLeaf
    )

    $raw = [string]$Value
    $raw = ($raw -replace '[\r\n]+', '').Trim().Trim('"')

    $pattern = '(?i)[A-Z]:\\[^\r\n]*' + [regex]::Escape($ExpectedLeaf)
    $match = [regex]::Match($raw, $pattern)
    if ($match.Success) {
        return ($match.Value -replace '[\r\n]+', '').Trim().Trim('"')
    }

    $firstLine = (($raw -split "`r?`n") | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -First 1)
    return (([string]$firstLine) -replace '[\r\n]+', '').Trim().Trim('"')
}

$vsDevCmd = Get-VsDevCmdPath
if (-not $vsDevCmd) {
    throw "VsDevCmd.bat not found. Install Visual Studio Build Tools with C++ workload and Windows SDK."
}
$vsDevCmd = Normalize-PathValue -Value $vsDevCmd -ExpectedLeaf "VsDevCmd.bat"

if ($InstallMissingTools) {
    Ensure-ScoopPackage -PackageName "rustup"
    Ensure-ScoopPackage -PackageName "llvm"
    Ensure-ScoopPackage -PackageName "nasm"
}

$llvmBin = Get-LlvmBinPath
if (-not $llvmBin -and $InstallMissingTools) {
    Ensure-ScoopPackage -PackageName "llvm"
    $llvmBin = Get-LlvmBinPath
}

if (-not $llvmBin) {
    throw "LLVM/libclang not found. Install LLVM (e.g. 'scoop install llvm') and retry."
}
$llvmBin = Normalize-PathValue -Value $llvmBin -ExpectedLeaf "bin"

$nasm = Get-NasmCommand
if (-not $nasm -and $InstallMissingTools) {
    Ensure-ScoopPackage -PackageName "nasm"
    $nasm = Get-NasmCommand
}

if (-not $nasm) {
    throw "NASM not found. Install NASM (e.g. 'scoop install nasm') and retry."
}

$cargo = Get-CargoCommand
if (-not $cargo -and $InstallMissingTools) {
    Ensure-ScoopPackage -PackageName "rustup"
    $cargo = Get-CargoCommand
}

if (-not $cargo) {
    throw "cargo not found in PATH."
}

$cargoPath = if ($cargo -is [System.IO.FileInfo]) { $cargo.FullName } else { $cargo.Source }
$cargoPath = Normalize-PathValue -Value $cargoPath -ExpectedLeaf "cargo.exe"
$cargoArgsEscaped = ($CargoArgs | ForEach-Object {
        if ($_ -match '[\s"]') {
            '"' + $_.Replace('"', '\"') + '"'
        }
        else {
            $_
        }
    }) -join ' '

Write-Host "Using VsDevCmd: $vsDevCmd"
Write-Host "Using LLVM: $llvmBin"
Write-Host "Using NASM: $(if ($nasm -is [System.IO.FileInfo]) { $nasm.FullName } else { $nasm.Source })"
Write-Host "Running: cargo $($CargoArgs -join ' ')"

$cmdFile = Join-Path ([IO.Path]::GetTempPath()) ("surreal-cargo-check-" + [Guid]::NewGuid().ToString("N") + ".cmd")
$cmdContent = "@echo off`r`n"
$cmdContent += 'call "' + $vsDevCmd + '" -arch=amd64 -host_arch=amd64 >nul' + "`r`n"
$cmdContent += 'if errorlevel 1 exit /b %errorlevel%' + "`r`n"
$cmdContent += 'set "LIBCLANG_PATH=' + $llvmBin + '"' + "`r`n"
$cmdContent += 'set "CLANG_PATH=' + (Join-Path $llvmBin "clang.exe") + '"' + "`r`n"
$cmdContent += 'set "PATH=' + $llvmBin + ';%PATH%"' + "`r`n"
$cmdContent += '"' + $cargoPath + '" ' + $cargoArgsEscaped + "`r`n"
$cmdContent += 'exit /b %errorlevel%' + "`r`n"

Set-Content -LiteralPath $cmdFile -Value $cmdContent -Encoding Ascii -NoNewline
if ($ShowGeneratedCmd) {
    Write-Host "Generated cmd file: $cmdFile"
    Get-Content -LiteralPath $cmdFile | ForEach-Object { Write-Host $_ }
}
try {
    & cmd.exe /d /c $cmdFile
    exit $LASTEXITCODE
}
finally {
    if (-not $ShowGeneratedCmd) {
        Remove-Item -LiteralPath $cmdFile -Force -ErrorAction SilentlyContinue
    }
}
