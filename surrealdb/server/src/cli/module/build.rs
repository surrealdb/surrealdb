use std::path::{Path, PathBuf};
use std::process::Command;
use std::{fs, mem};

use anyhow::Result;
use surrealism_runtime::PrefixErr;
use surrealism_runtime::config::{AbiVersion, SurrealismConfig, Target};
use surrealism_runtime::exports::{ExportsManifest, FunctionExport};
use surrealism_runtime::package::SurrealismPackage;
use surrealism_runtime::runtime::Runtime;
use tempfile::TempDir;
use wasm_encoder::{ComponentSectionId, Encode, RawSection, Section};
use wasm_opt::OptimizationOptions;

/// Build a Surrealism WASM module from a Rust project, optimize the binary,
/// and pack it into a `.surrealism` package file.
pub async fn init(path: Option<PathBuf>, out: Option<PathBuf>, debug: bool) -> Result<()> {
	let path = match path {
		Some(p) => p,
		None => std::env::current_dir().prefix_err(|| "Failed to determine current directory")?,
	};
	let config = load_config(&path)?;

	if config.target != Target::Rust {
		anyhow::bail!(
			"Unsupported target '{}'. Only 'rust' is currently supported.",
			config.target
		);
	}

	let metadata = metadata(&path).prefix_err(|| "Failed to retrieve cargo metadata")?;
	validate_sdk_version(&metadata)?;
	let (package_name, source_wasm) = get_source_wasm(&metadata, &path, debug)?;

	build_wasm_module(&path, &package_name, debug)?;

	if !source_wasm.exists() {
		anyhow::bail!("Expected WASM file not found: {}", source_wasm.display());
	}
	let compiled = fs::read(&source_wasm).prefix_err(|| "Failed to read WASM file")?;

	let wasm = if debug {
		compiled
	} else {
		println!("Optimizing bundle...");
		optimize_component(&compiled)?
	};

	let fs_dir = resolve_attach_fs(&path, &config)?;
	let logo = resolve_logo(&path)?;

	println!("Extracting function signatures...");
	let exports = extract_exports(&wasm, &config).await?;

	let package = SurrealismPackage {
		config,
		wasm,
		exports,
		fs: None,
		logo,
	};
	let out = resolve_output_path(out, &package.config)?;
	package.pack(out, fs_dir.as_deref()).prefix_err(|| "Failed to pack Surrealism package")?;

	Ok(())
}

/// Instantiate the compiled WASM temporarily to extract function signatures
/// into an `ExportsManifest`.
async fn extract_exports(wasm: &[u8], config: &SurrealismConfig) -> Result<ExportsManifest> {
	use crate::cli::module::host::DemoHost;

	let temp_package = SurrealismPackage {
		config: config.clone(),
		wasm: wasm.to_vec(),
		exports: ExportsManifest::empty(),
		fs: None,
		logo: None,
	};

	let runtime = Runtime::new(temp_package, 1, None, None, None, None)?;
	let host = Box::new(DemoHost::new());
	let mut controller =
		runtime.new_controller(host).await.prefix_err(|| "Failed to load WASM module")?;

	let names = controller.list().await.prefix_err(|| "Failed to list functions")?;

	let mut functions = Vec::new();
	for name in names {
		let display = name.as_deref().unwrap_or("<default>");
		let args = controller
			.args(name.clone())
			.await
			.prefix_err(|| format!("Failed to get args for '{display}'"))?;
		let returns = controller
			.returns(name.clone())
			.await
			.prefix_err(|| format!("Failed to get return type for '{display}'"))?;
		let writeable = controller
			.writeable(name.clone())
			.await
			.prefix_err(|| format!("Failed to get writeable flag for '{display}'"))?;
		let comment = controller
			.comment(name.clone())
			.await
			.prefix_err(|| format!("Failed to get comment for '{display}'"))?;

		let args_text = Some(args.iter().map(|(n, k)| format!("{n}: {k}")).collect());
		let returns_text = Some(returns.to_string());

		functions.push(FunctionExport {
			name,
			args,
			returns,
			args_text,
			returns_text,
			writeable,
			comment,
		});
	}

	Ok(ExportsManifest {
		functions,
	})
}

/// Parse the `surrealism.toml` configuration from the project directory.
///
/// The ABI version is always stamped to `AbiVersion::CURRENT` because the
/// build toolchain targets the current SDK; user-supplied values are ignored.
fn load_config(path: &Path) -> Result<SurrealismConfig> {
	let surrealism_toml = path.join("surrealism.toml");
	if !surrealism_toml.exists() {
		anyhow::bail!("surrealism.toml not found in the current directory");
	}

	let mut config = SurrealismConfig::parse(
		&fs::read_to_string(&surrealism_toml).prefix_err(|| "Failed to read surrealism.toml")?,
	)?;
	config.abi = AbiVersion::CURRENT;
	Ok(config)
}

/// Verify that the project's resolved `surrealism` crate version is compatible
/// with the version this toolchain was compiled against. Compatibility follows
/// Cargo's caret (`^`) semantics: the SDK must be at least the toolchain
/// version but within the same semver-compatible range.
///
/// For pre-1.0 (`0.x.y`): minor must match, patch may be ahead.
/// For post-1.0: major must match, minor/patch may be ahead.
fn validate_sdk_version(metadata: &serde_json::Value) -> Result<()> {
	let expected = surrealism_runtime::SDK_VERSION;

	let packages = metadata["packages"]
		.as_array()
		.ok_or_else(|| anyhow::anyhow!("No packages found in cargo metadata"))?;

	let surrealism_pkg = packages.iter().find(|pkg| pkg["name"].as_str() == Some("surrealism"));

	let Some(pkg) = surrealism_pkg else {
		anyhow::bail!(
			"surrealism dependency not found in the project's dependency tree.\n\
			 Ensure your Cargo.toml depends on the `surrealism` crate (version {expected})."
		);
	};

	let resolved_str = pkg["version"]
		.as_str()
		.ok_or_else(|| anyhow::anyhow!("surrealism package in metadata has no version"))?;

	let resolved: semver::Version = resolved_str
		.parse()
		.map_err(|e| anyhow::anyhow!("invalid surrealism version '{resolved_str}': {e}"))?;

	let req: semver::VersionReq = format!("^{expected}")
		.parse()
		.map_err(|e| anyhow::anyhow!("invalid toolchain SDK version '{expected}': {e}"))?;

	if !req.matches(&resolved) {
		anyhow::bail!(
			"surrealism SDK version mismatch: project uses {resolved}, \
			 but this toolchain requires ^{expected} (>={expected}, compatible range).\n\
			 Update your surrealism dependency to version {expected}."
		);
	}

	Ok(())
}

const WASM_TARGET: &str = "wasm32-wasip2";

/// Invoke `cargo build` targeting `wasm32-wasip2`.
fn build_wasm_module(path: &Path, package_name: &str, debug: bool) -> Result<()> {
	let target = WASM_TARGET;
	let profile = if debug {
		"debug"
	} else {
		"release"
	};
	println!("Building WASM module (target: {target}, profile: {profile})...");

	let mut cmd = Command::new("cargo");
	cmd.args(["build", "-p", package_name, "--target", target]);
	if !debug {
		cmd.arg("--release");
	}

	let cargo_status =
		cmd.current_dir(path).status().prefix_err(|| "Failed to execute cargo build")?;

	if !cargo_status.success() {
		anyhow::bail!("Cargo build failed");
	}

	Ok(())
}

/// Returns `true` if a custom section with the given name should be removed
/// from a release build. Preserves `component-type:*` sections which are
/// required by the component model.
fn should_strip_section(name: &str) -> bool {
	if name.starts_with("component-type") {
		return false;
	}
	name.starts_with(".debug")
		|| name == "name"
		|| name == "sourceMappingURL"
		|| name.starts_with("reloc.")
		|| name.starts_with("linking")
		|| name == "target_features"
		|| name == "producers"
}

/// Optimize a P2 component binary by stripping unnecessary custom sections
/// at all nesting levels and applying wasm-opt to embedded core modules.
///
/// `walrus` and `wasm-opt` only understand core WASM modules, not the
/// component model binary format. This function uses `wasmparser` to walk
/// the component's nested structure, strips metadata sections, and runs
/// Binaryen on each core module before re-embedding it.
///
/// The parse-rewrite approach is taken from the Bytecode Alliance's
/// [`wasm-tools strip`](https://github.com/bytecodealliance/wasm-tools/blob/main/src/bin/wasm-tools/strip.rs).
fn optimize_component(wasm_bytes: &[u8]) -> Result<Vec<u8>> {
	let mut output = Vec::new();
	let mut stack = Vec::new();

	for payload in wasmparser::Parser::new(0).parse_all(wasm_bytes) {
		let payload = payload.prefix_err(|| "Failed to parse WASM component")?;

		match &payload {
			wasmparser::Payload::Version {
				encoding,
				..
			} => {
				output.extend_from_slice(match encoding {
					wasmparser::Encoding::Component => &wasm_encoder::Component::HEADER,
					wasmparser::Encoding::Module => &wasm_encoder::Module::HEADER,
				});
				continue;
			}
			wasmparser::Payload::ModuleSection {
				..
			}
			| wasmparser::Payload::ComponentSection {
				..
			} => {
				stack.push(mem::take(&mut output));
				continue;
			}
			wasmparser::Payload::End(_) => {
				let mut parent = match stack.pop() {
					Some(p) => p,
					None => break,
				};
				let is_component = output.starts_with(&wasm_encoder::Component::HEADER);
				if !is_component {
					output = apply_wasm_opt(&output)?;
				}
				if is_component {
					parent.push(ComponentSectionId::Component as u8);
				} else {
					parent.push(ComponentSectionId::CoreModule as u8);
				}
				output.encode(&mut parent);
				output = parent;
				continue;
			}
			wasmparser::Payload::CustomSection(c) => {
				if should_strip_section(c.name()) {
					continue;
				}
			}
			_ => {}
		}

		if let Some((id, range)) = payload.as_section() {
			RawSection {
				id,
				data: &wasm_bytes[range],
			}
			.append_to(&mut output);
		}
	}

	Ok(output)
}

/// Run Binaryen's `wasm-opt` on a core module with aggressive size optimization
/// and common post-MVP features enabled.
fn apply_wasm_opt(wasm_bytes: &[u8]) -> Result<Vec<u8>> {
	let mut opts = OptimizationOptions::new_optimize_for_size_aggressively();
	opts.enable_feature(wasm_opt::Feature::BulkMemory);
	opts.enable_feature(wasm_opt::Feature::Simd);
	opts.enable_feature(wasm_opt::Feature::Atomics);
	opts.enable_feature(wasm_opt::Feature::MutableGlobals);
	opts.enable_feature(wasm_opt::Feature::TruncSat);
	opts.enable_feature(wasm_opt::Feature::SignExt);
	opts.debug_info(false);

	let temp_dir = TempDir::new().prefix_err(|| "Failed to create temporary directory")?;
	let temp_wasm_input = temp_dir.path().join("input.wasm");
	let temp_wasm_output = temp_dir.path().join("output.wasm");

	fs::write(&temp_wasm_input, wasm_bytes).prefix_err(|| "Failed to write temporary WASM file")?;

	opts.run(&temp_wasm_input, &temp_wasm_output)
		.prefix_err(|| "Failed to optimize WASM with wasm-opt")?;

	Ok(fs::read(&temp_wasm_output).prefix_err(|| "Failed to read optimized WASM file")?)
}

/// Locate the `.wasm` artifact produced by `cargo build` using `cargo metadata`
/// to resolve the target directory and package name.
fn get_source_wasm(
	metadata: &serde_json::Value,
	path: &Path,
	debug: bool,
) -> Result<(String, PathBuf)> {
	let target_directory = metadata["target_directory"]
		.as_str()
		.ok_or_else(|| anyhow::anyhow!("No target_directory found in cargo metadata"))?;

	let packages = metadata["packages"]
		.as_array()
		.ok_or_else(|| anyhow::anyhow!("No packages found in cargo metadata"))?;

	let current_package = packages
		.iter()
		.find(|pkg| {
			let manifest_path = pkg["manifest_path"].as_str().unwrap_or("");
			let manifest_pathbuf = PathBuf::from(manifest_path);
			let manifest_dir = manifest_pathbuf.parent();

			match (manifest_dir, path.canonicalize()) {
				(Some(manifest_dir), Ok(canonical_path)) => {
					manifest_dir.canonicalize().ok() == Some(canonical_path)
				}
				_ => false,
			}
		})
		.ok_or_else(|| {
			anyhow::anyhow!(
				"Could not find current package in metadata for path: {}",
				path.display()
			)
		})?;

	let package_name = current_package["name"]
		.as_str()
		.ok_or_else(|| anyhow::anyhow!("No package name found in metadata"))?;

	let wasm_filename = format!("{}.wasm", package_name.replace("-", "_"));
	let target = WASM_TARGET;
	let profile_dir = if debug {
		"debug"
	} else {
		"release"
	};
	let target_dir = PathBuf::from(target_directory).join(format!("{target}/{profile_dir}"));
	Ok((package_name.to_string(), target_dir.join(&wasm_filename)))
}

/// Run `cargo metadata` and return the parsed JSON.
///
/// Uses the full resolve (no `--no-deps`) so that transitive dependency
/// versions are available for SDK compatibility checks.
fn metadata(path: &Path) -> Result<serde_json::Value> {
	let output = Command::new("cargo")
		.args(["metadata", "--format-version", "1"])
		.current_dir(path)
		.output()
		.prefix_err(|| "Failed to execute cargo metadata")?;

	if !output.status.success() {
		anyhow::bail!("Failed to get cargo metadata");
	}

	let metadata_str =
		String::from_utf8(output.stdout).prefix_err(|| "Invalid UTF-8 in cargo metadata output")?;

	Ok(serde_json::from_str(&metadata_str).prefix_err(|| "Failed to parse cargo metadata JSON")?)
}

/// Read `logo.png` from the project root if it exists, validating format and size.
fn resolve_logo(project_root: &Path) -> Result<Option<Vec<u8>>> {
	use surrealism_runtime::package::{MAX_LOGO_BYTES, verify_logo};

	let logo_path = project_root.join("logo.png");
	if !logo_path.is_file() {
		return Ok(None);
	}

	let bytes = fs::read(&logo_path).prefix_err(|| "Failed to read logo.png")?;
	verify_logo(&bytes).prefix_err(|| {
		format!("Invalid logo.png (must be a valid PNG, max {} KiB)", MAX_LOGO_BYTES / 1024)
	})?;

	println!("Including logo.png ({} bytes)", bytes.len());
	Ok(Some(bytes))
}

/// Resolve the `[attach] fs` directory path from the config.
///
/// Returns `Some(path)` when a filesystem directory should be bundled, or
/// `None` when no `[attach] fs` is configured.
fn resolve_attach_fs(project_root: &Path, config: &SurrealismConfig) -> Result<Option<PathBuf>> {
	let Some(ref fs_value) = config.attach.fs else {
		return Ok(None);
	};

	if fs_value.is_empty() {
		return Ok(None);
	}

	let fs_path = PathBuf::from(fs_value);
	let resolved = if fs_path.is_absolute() {
		fs_path
	} else {
		project_root.join(&fs_path)
	};

	if !resolved.exists() {
		anyhow::bail!(
			"Attached filesystem directory not found: {} (resolved to {})",
			fs_value,
			resolved.display()
		);
	}
	if !resolved.is_dir() {
		anyhow::bail!(
			"Attached filesystem path is not a directory: {} (resolved to {})",
			fs_value,
			resolved.display()
		);
	}

	Ok(Some(resolved))
}

/// Resolve the output path for the `.surrealism` package, defaulting to
/// `<package_name>.surrealism` in the current working directory.
fn resolve_output_path(out: Option<PathBuf>, config: &SurrealismConfig) -> Result<PathBuf> {
	let cwd = || std::env::current_dir().prefix_err(|| "Failed to determine current directory");
	match out {
		None => Ok(cwd()?.join(config.file_name())),
		Some(out_path) => {
			if out_path.is_absolute() {
				Ok(out_path)
			} else {
				Ok(cwd()?.join(out_path))
			}
		}
	}
}
