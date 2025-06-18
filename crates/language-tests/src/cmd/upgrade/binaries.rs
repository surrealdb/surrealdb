use std::{io::Write, path::Path, process::Stdio};

use anyhow::{Context, bail};
use semver::Version;
use serde_json::Value;
use tokio::process::Command;

use crate::cli::DsVersion;

pub async fn actual_version(version: DsVersion) -> anyhow::Result<Version> {
	match version {
		DsVersion::Version(x) => Ok(x),
		DsVersion::Path(ref x) => retrieve_version_from_path(x).await,
	}
}

pub async fn prepare(version: DsVersion, download_permission: bool) -> anyhow::Result<()> {
	match version {
		DsVersion::Version(x) => prepare_version(x, download_permission).await,
		DsVersion::Path(ref x) => prepare_path(x).await,
	}
}

pub async fn prepare_version(version: Version, download_permission: bool) -> anyhow::Result<()> {
	if Path::new(".binary_cache").join(format!("surreal-v{version}")).exists() {
		return Ok(());
	}

	if !Path::new(".binary_cache").exists() {
		tokio::fs::create_dir(".binary_cache")
			.await
			.context("Failed to create binary cache directory")?;
	}

	#[cfg(not(target_os = "windows"))]
	if Command::new("tar")
		.kill_on_drop(true)
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.output()
		.await
		.is_err()
	{
		bail!("Can't find the tar utility, tar is required to be able to unzip downloaded binaries")
	}

	if Command::new("curl")
		.kill_on_drop(true)
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.output()
		.await
		.is_err()
	{
		return prepare_curl(version.clone(), download_permission).await;
	}

	if Command::new("wget")
		.kill_on_drop(true)
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.output()
		.await
		.is_ok()
	{
		return prepare_wget(version.clone(), download_permission).await;
	}

	bail!(
		"Could not run wget or curl, please install either curl or wget to facilitate downloading surrealdb binaries"
	)
}

cfg_if::cfg_if! {
	if #[cfg(all(target_os = "macos",target_arch = "x86_64"))]{
		fn platform_name() -> &'static str{
			"darwin-amd64.tgz"
		}
	}else if #[cfg(all(target_os = "macos",target_arch = "aarch64"))]{
		fn platform_name() -> &'static str{
			"darwin-arm64.tgz"
		}
	}else if #[cfg(all(target_os = "windows",target_arch = "x86_64"))]{
		fn platform_name() -> &'static str{
			"windows-amd64.exe"
		}
	}else if #[cfg(all(target_family = "unix",target_arch = "x86_64"))]{
		fn platform_name() -> &'static str{
			"linux-amd64.tgz"
		}
	}else if #[cfg(all(target_family = "unix",target_arch = "aarch64"))]{
		fn platform_name() -> &'static str{
			"linux-arm64.tgz"
		}
	}else {
		fn platform_name() -> &'static str{
			compile_error!("Platform is not supported by the upgrade tests")
		}
	}
}

cfg_if::cfg_if! {
	if #[cfg(target_os = "windows")] {
		fn download_path(version: &Version) -> String{
			format!(".binary_cache/surreal-v{version}.exe")
		}
	}else{
		fn download_path(version: &Version) -> String{
			format!(".binary_cache/surreal-v{version}.tgz")
		}
	}
}

fn binary_url(version: &Version) -> String {
	format!(
		"https://github.com/surrealdb/surrealdb/releases/download/v{version}/surreal-v{version}.{}",
		platform_name()
	)
}

fn ask_download_permission(url: &str) -> anyhow::Result<()> {
	println!("> Need to download file from `{}`", url);
	let mut lines = std::io::stdin().lines();
	loop {
		print!("> Proceed? [Y/n] ");
		std::io::stdout().flush().unwrap();
		let Some(line) = lines.next() else {
			bail!("Cancelled")
		};
		let line = line.context("Failed to read line from stdin")?;
		let line = line.trim().to_lowercase();
		if line == "y" || line.is_empty() {
			return Ok(());
		}
		if line == "n" {
			bail!("Couldn't download required file")
		}
		println!("Please enter either 'y' or 'n'")
	}
}

pub async fn prepare_curl(version: Version, permission: bool) -> anyhow::Result<()> {
	let url = binary_url(&version);

	if !permission {
		ask_download_permission(&url)?;
	}

	let mut curl = Command::new("curl")
		.args(["--fail", "--location", "--output", &download_path(&version), &url])
		.stdout(Stdio::inherit())
		.stderr(Stdio::inherit())
		.spawn()
		.context("failed to spawn download command")?;

	let output = curl.wait().await.context("failed to await download command")?;

	if !output.success() {
		bail!("Downloading binary failed")
	}

	#[cfg(not(target_os = "windows"))]
	unzip(version).await?;

	Ok(())
}

pub async fn prepare_wget(version: Version, permission: bool) -> anyhow::Result<()> {
	let url = binary_url(&version);
	if !permission {
		ask_download_permission(&url)?;
	}

	let mut wget = Command::new("wget")
		.args(["--output-document", &download_path(&version), &url])
		.stdout(Stdio::inherit())
		.stderr(Stdio::inherit())
		.spawn()
		.context("failed to spawn download command")?;

	let output = wget.wait().await.context("failed to await download command")?;

	if !output.success() {
		bail!("Downloading binary failed")
	}

	#[cfg(not(target_os = "windows"))]
	unzip(version).await?;

	Ok(())
}

#[cfg(not(target_os = "windows"))]
async fn unzip(version: Version) -> anyhow::Result<()> {
	let mut command = Command::new("tar")
		.args(["--directory", ".binary_cache"])
		.args(["--transform", &format!("s/surreal/surreal-v{version}/g")])
		.arg("-xvf")
		.arg(download_path(&version))
		.arg("surreal")
		.stdout(Stdio::inherit())
		.stderr(Stdio::inherit())
		.spawn()
		.context("Failed to spawn unzip command")?;

	let out = command.wait().await.context("Failed to wait on unzip command")?;
	if !out.success() {
		bail!("Unzip command was not successfull");
	}
	tokio::fs::remove_file(download_path(&version))
		.await
		.context("Failed to remove downloaded archive after unzipping")?;
	Ok(())
}

pub async fn retrieve_version_from_path(path: &str) -> anyhow::Result<Version> {
	let output = Command::new("cargo")
		.current_dir(path)
		.args(["metadata", "--format-version", "1", "--no-deps"])
		.output()
		.await
		.with_context(|| {
			format!("failed to 'cargo metadata', could not find surrealdb version for '{path}'")
		})?;

	let text = String::from_utf8(output.stdout)
		.context("Command 'cargo metadata' returned a non-utf8 string")?;

	let json: Value =
		serde_json::from_str(&text).context("Failed to parser 'cargo metadata' json output")?;

	let version = json
		.get("packages")
		.and_then(|x| x.as_array())
		.and_then(|x| {
			x.iter().find(|x| x.get("name").and_then(|x| x.as_str()) == Some("surrealdb"))
		})
		.and_then(|x| x.get("version"))
		.and_then(|x| x.as_str())
		.ok_or_else(|| anyhow::anyhow!("Could not find 'surrealdb' package in rust workspace"))?;

	Version::parse(version).context("Failed to parse 'surreealdb' package config")
}

pub async fn prepare_path(path: &str) -> anyhow::Result<()> {
	let mut child = Command::new("cargo")
		.current_dir(path)
		.args(["build", "--bin", "surreal"])
		.stderr(Stdio::inherit())
		.stdout(Stdio::inherit())
		.spawn()
		.with_context(|| {
			format!("failed to spawn build command, could not build directory '{path}'")
		})?;
	let output = child.wait().await.context("failed to wait for build command")?;

	if !output.success() {
		bail!("Build command failed")
	}

	Ok(())
}
