<#
.SYNOPSIS
Sets up a Windows MSVC build environment for SurrealDB dependencies.

.DESCRIPTION
This script verifies and installs missing tools required to build
SurrealDB on Windows:
- Visual Studio Build Tools (detected, not auto-installed),
- Rust toolchain (rustup/rustc/cargo),
- LLVM + libclang,
- NASM,
- CMake.

If Scoop is missing, this script can bootstrap Scoop and then install missing
user-space dependencies (rustup, llvm, nasm, cmake).

This script only prepares and validates the environment. It does not run cargo.

.EXAMPLE
pwsh -File .\dev\windows\windows-setup-build-env.ps1
#>


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

    return $null
}

function Get-LlvmBinPath {
    $candidates = @(
        (Join-Path $env:USERPROFILE "scoop\apps\llvm\current\bin"),
        (Join-Path $env:ProgramFiles "LLVM\bin")
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

function Get-ScoopCommand {
    $scoop = Get-Command scoop -ErrorAction SilentlyContinue
    if ($scoop) {
        return $scoop
    }

    $fallbacks = @(
        (Join-Path $env:USERPROFILE "scoop\shims\scoop.cmd"),
        (Join-Path $env:USERPROFILE "scoop\shims\scoop.ps1")
    )

    foreach ($fallback in $fallbacks) {
        if (Test-Path -LiteralPath $fallback -PathType Leaf) {
            return Get-Item -LiteralPath $fallback
        }
    }

    return $null
}

function Ensure-ScoopInstalled {
    $scoop = Get-ScoopCommand
    if ($scoop) {
        return $scoop
    }

    Write-Host "Scoop not found. Installing Scoop for current user..."
    try {
        $script = Invoke-RestMethod -Uri "https://get.scoop.sh"
        Invoke-Expression "& { $script } -RunAsAdmin:`$false"
    }
    catch {
        throw "Failed to install Scoop automatically. Install Scoop manually from https://scoop.sh and retry. Error: $($_.Exception.Message)"
    }

    $scoopShims = Join-Path $env:USERPROFILE "scoop\shims"
    if ((Test-Path -LiteralPath $scoopShims -PathType Container) -and -not ($env:PATH -split ';' | Where-Object { $_ -eq $scoopShims })) {
        $env:PATH = "$scoopShims;$env:PATH"
    }

    $scoop = Get-ScoopCommand
    if (-not $scoop) {
        throw "Scoop installation completed but command is still unavailable. Open a new shell and retry."
    }

    return $scoop
}

function Ensure-ScoopPackage {
    param(
        [string]$PackageName
    )

    $scoop = Ensure-ScoopInstalled
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

function Get-RustupCommand {
    $rustup = Get-Command rustup -ErrorAction SilentlyContinue
    if ($rustup) {
        return $rustup
    }

    $fallbacks = @(
        (Join-Path $env:USERPROFILE ".cargo\bin\rustup.exe"),
        (Join-Path $env:USERPROFILE "scoop\shims\rustup.exe")
    )

    foreach ($fallback in $fallbacks) {
        if (Test-Path -LiteralPath $fallback -PathType Leaf) {
            return Get-Item -LiteralPath $fallback
        }
    }

    return $null
}

function Get-RustcCommand {
    $rustc = Get-Command rustc -ErrorAction SilentlyContinue
    if ($rustc) {
        return $rustc
    }

    $fallbacks = @(
        (Join-Path $env:USERPROFILE ".cargo\bin\rustc.exe"),
        (Join-Path $env:USERPROFILE "scoop\shims\rustc.exe")
    )

    foreach ($fallback in $fallbacks) {
        if (Test-Path -LiteralPath $fallback -PathType Leaf) {
            return Get-Item -LiteralPath $fallback
        }
    }

    return $null
}

function Ensure-RustToolchainAvailable {
    $rustup = Get-RustupCommand
    if (-not $rustup) {
        Ensure-ScoopPackage -PackageName "rustup"
        $rustup = Get-RustupCommand
    }

    if (-not $rustup) {
        throw "rustup not found after installation attempt."
    }

    $rustupPath = if ($rustup -is [System.IO.FileInfo]) { $rustup.FullName } else { $rustup.Source }

    $rustc = Get-RustcCommand
    $cargo = Get-CargoCommand
    if ($rustc -and $cargo) {
        return
    }

    Write-Host "Installing stable Rust toolchain via rustup..."
    & $rustupPath toolchain install stable
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install stable toolchain via rustup."
    }

    & $rustupPath default stable
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to set stable as default toolchain via rustup."
    }

    $rustc = Get-RustcCommand
    if (-not $rustc) {
        throw "rustc not found after rustup update."
    }
}

function Get-NasmCommand {
    $nasm = Get-Command nasm -ErrorAction SilentlyContinue
    if ($nasm) {
        return $nasm
    }

    $fallbacks = @(
        (Join-Path $env:USERPROFILE "scoop\shims\nasm.exe"),
        (Join-Path $env:ProgramFiles "NASM\nasm.exe")
    )

    foreach ($fallback in $fallbacks) {
        if (Test-Path -LiteralPath $fallback -PathType Leaf) {
            return Get-Item -LiteralPath $fallback
        }
    }

    return $null
}

function Get-CmakeCommand {
    $cmake = Get-Command cmake -ErrorAction SilentlyContinue
    if ($cmake) {
        return $cmake
    }

    $fallbacks = @(
        (Join-Path $env:USERPROFILE "scoop\shims\cmake.exe"),
        (Join-Path $env:ProgramFiles "CMake\bin\cmake.exe")
    )

    foreach ($fallback in $fallbacks) {
        if (Test-Path -LiteralPath $fallback -PathType Leaf) {
            return Get-Item -LiteralPath $fallback
        }
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
    throw "VsDevCmd.bat not found via vswhere. Install Visual Studio Build Tools (C++ workload + Windows SDK)."
}
$vsDevCmd = Normalize-PathValue -Value $vsDevCmd -ExpectedLeaf "VsDevCmd.bat"

Ensure-RustToolchainAvailable

$llvmBin = Get-LlvmBinPath
if (-not $llvmBin) {
    Ensure-ScoopPackage -PackageName "llvm"
    $llvmBin = Get-LlvmBinPath
}

if (-not $llvmBin) {
    throw "LLVM/libclang not found. Install LLVM (e.g. 'scoop install llvm') and retry."
}
$llvmBin = Normalize-PathValue -Value $llvmBin -ExpectedLeaf "bin"

$nasm = Get-NasmCommand
if (-not $nasm) {
    Ensure-ScoopPackage -PackageName "nasm"
    $nasm = Get-NasmCommand
}

if (-not $nasm) {
    throw "NASM not found. Install NASM (e.g. 'scoop install nasm') and retry."
}

$cmake = Get-CmakeCommand
if (-not $cmake) {
    Ensure-ScoopPackage -PackageName "cmake"
    $cmake = Get-CmakeCommand
}

if (-not $cmake) {
    throw "CMake not found. Install CMake (e.g. 'scoop install cmake') and retry."
}

$cargo = Get-CargoCommand
if (-not $cargo) {
    Ensure-ScoopPackage -PackageName "rustup"
    $cargo = Get-CargoCommand
}

if (-not $cargo) {
    throw "cargo not found in PATH. Install Rust with rustup and retry."
}

$cargoPath = if ($cargo -is [System.IO.FileInfo]) { $cargo.FullName } else { $cargo.Source }
$cargoPath = Normalize-PathValue -Value $cargoPath -ExpectedLeaf "cargo.exe"

Write-Host "Using VsDevCmd: $vsDevCmd"
Write-Host "Using LLVM: $llvmBin"
Write-Host "Using NASM: $(if ($nasm -is [System.IO.FileInfo]) { $nasm.FullName } else { $nasm.Source })"
Write-Host "Using CMake: $(if ($cmake -is [System.IO.FileInfo]) { $cmake.FullName } else { $cmake.Source })"
Write-Host "Using Cargo: $cargoPath"
Write-Host ""
Write-Host "Build environment prerequisites are installed and verified."
Write-Host "Next step: run 'cargo build' in your terminal."
Write-Host "If a plain shell cannot find MSVC tools, use a Developer PowerShell for VS or run VsDevCmd first."
