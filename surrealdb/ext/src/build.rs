use std::path::{Path, PathBuf};
use std::process::Command;
use std::{fs, mem};

use anyhow::Result;
use surrealism_runtime::PrefixErr;
use surrealism_runtime::config::{AbiVersion, FsMounts, PACKAGE_FILE, SurrealismConfig, Target};
use surrealism_runtime::epoch::{EngineSettings, engines};
use surrealism_runtime::exports::{ExportsManifest, FunctionExport};
use surrealism_runtime::package::SurrealismPackage;
use surrealism_runtime::package_config::PackageConfig;
use surrealism_runtime::runtime::{InstanceSettings, Runtime};
use tempfile::TempDir;
use wasm_encoder::{ComponentSectionId, Encode, RawSection, Section};
use wasm_opt::OptimizationOptions;

use super::snapshot;

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

	let attached = resolve_attach_fs(&path, &config)?;
	let fs_dir = attached.as_ref().map(AttachedFs::path);
	let logo = resolve_logo(&path)?;

	// Before anything reads the component back, so the exports manifest is
	// taken from, and the archive packs, exactly what will run.
	println!("Pre-initialising...");
	let wasm = match snapshot::snapshot(&wasm, fs_dir)
		.await
		.map_err(|e| anyhow::anyhow!("Failed to pre-initialise the module: {e:#}"))?
	{
		Some(snapshot) => {
			for read in &snapshot.reads {
				println!(
					"Note: `init` read {read}. The value is frozen into the module, so every \
					 invocation sees the same one."
				);
			}
			snapshot.wasm
		}
		None => wasm,
	};

	println!("Extracting function signatures...");
	let exports = extract_exports(&wasm, &config).await?;

	let package = SurrealismPackage {
		config,
		wasm,
		exports,
		fs: None,
		logo,
		manifest: None,
		signature: None,
	};
	let out = resolve_output_path(out, &package.config)?;
	let digest =
		package.pack(out.clone(), fs_dir).prefix_err(|| "Failed to pack Surrealism package")?;
	println!("Packed {} (digest {digest})", out.display());

	Ok(())
}

/// Instantiate the compiled WASM temporarily to extract function signatures
/// into an `ExportsManifest`.
///
/// `wasm` is the snapshot taken after `init`, so its memory already holds the
/// functions `init` added with `surrealism::register`, and the introspection
/// exports list them beside the `#[surrealism]` ones.
async fn extract_exports(wasm: &[u8], config: &SurrealismConfig) -> Result<ExportsManifest> {
	let temp_package = SurrealismPackage {
		config: config.clone(),
		wasm: wasm.to_vec(),
		exports: ExportsManifest::empty(),
		fs: None,
		logo: None,
		manifest: None,
		signature: None,
	};

	let runtime = Runtime::new(
		temp_package,
		engines(EngineSettings {
			max_concurrent_invocations: 1,
			max_memory_bytes: None,
		}),
	)?;
	// Signature extraction only calls the sync introspection exports, which are
	// lent no transaction and need no network. It runs on the guarded engine,
	// the one `Runtime::new` compiles.
	let mut controller = runtime
		.instantiate(&InstanceSettings::local(), None, None)
		.await
		.prefix_err(|| "Failed to load WASM module")?;

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
		let declares_transaction = controller
			.declares_transaction(name.clone())
			.await
			.prefix_err(|| format!("Failed to get transaction declaration for '{display}'"))?;
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
			declares_transaction,
			comment,
		});
	}

	Ok(ExportsManifest {
		functions,
	})
}

/// The project's package file ([`PackageConfig::open`]), with a warning
/// naming a `surrealism.toml` that was not read because a `package.toml`
/// exists.
///
/// The ABI version is always stamped to `AbiVersion::CURRENT` because the
/// build toolchain targets the current SDK; user-supplied values are ignored.
fn load_config(path: &Path) -> Result<SurrealismConfig> {
	let mut package = PackageConfig::open(path)?;
	if let Some(ignored) = package.ignored() {
		eprintln!(
			"Warning: ignoring {} because {PACKAGE_FILE} is present; remove it to silence this warning",
			ignored.display()
		);
	}
	package.set_abi(AbiVersion::CURRENT);
	Ok(package.into_config())
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

/// Invoke `cargo build` targeting `wasm32-wasip2`.
fn build_wasm_module(path: &Path, package_name: &str, debug: bool) -> Result<()> {
	let target = crate::wasm_target::TARGET;
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

	// Inherited streams, so cargo's progress shows as it builds rather than all
	// at once at the end; its errors are already on the terminal on failure.
	let status = cmd.current_dir(path).status().prefix_err(|| "Failed to execute cargo build")?;

	if !status.success() {
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

		#[allow(clippy::collapsible_match)]
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

	let wasm_filename = wasm_filename(current_package, package_name);
	let target = crate::wasm_target::TARGET;
	let profile_dir = if debug {
		"debug"
	} else {
		"release"
	};
	let target_dir = PathBuf::from(target_directory).join(format!("{target}/{profile_dir}"));
	Ok((package_name.to_string(), target_dir.join(&wasm_filename)))
}

/// The `.wasm` file cargo builds for `package`: named after its library
/// target, which `[lib] name = "…"` can set apart from the package's name. A
/// `cdylib` target wins over a plain `lib`; with neither, the package name.
fn wasm_filename(package: &serde_json::Value, package_name: &str) -> String {
	let targets = package["targets"].as_array().map(Vec::as_slice).unwrap_or_default();
	let lib_named = |kind: &str| {
		targets
			.iter()
			.find(|target| {
				target["kind"]
					.as_array()
					.is_some_and(|kinds| kinds.iter().any(|k| k.as_str() == Some(kind)))
			})
			.and_then(|target| target["name"].as_str())
	};
	let lib_name = lib_named("cdylib").or_else(|| lib_named("lib")).unwrap_or(package_name);
	format!("{}.wasm", lib_name.replace('-', "_"))
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
		let stderr = String::from_utf8_lossy(&output.stderr);
		anyhow::bail!("Failed to get cargo metadata:\n{stderr}");
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

/// The directory packed as `surrealism/fs/`: the root mount's directory
/// itself when it is the only mount, or a temporary directory holding every
/// mount at its guest path.
enum AttachedFs {
	Dir(PathBuf),
	Assembled(TempDir),
}

impl AttachedFs {
	fn path(&self) -> &Path {
		match self {
			Self::Dir(dir) => dir,
			Self::Assembled(dir) => dir.path(),
		}
	}
}

/// The `[attach] fs` mounts, resolved against `project_root` and assembled
/// into one directory. `None` when nothing is attached.
///
/// A mount whose guest path already exists in a mount above it is refused,
/// so no file is silently shadowed.
fn resolve_attach_fs(project_root: &Path, config: &SurrealismConfig) -> Result<Option<AttachedFs>> {
	let Some(mounts) = &config.attach.fs else {
		return Ok(None);
	};
	let mut resolved = Vec::new();
	for (guest, local) in mounts.iter() {
		if local.is_empty() {
			continue;
		}
		let local_path = PathBuf::from(local);
		let dir = if local_path.is_absolute() {
			local_path
		} else {
			project_root.join(&local_path)
		};
		if !dir.is_dir() {
			anyhow::bail!(
				"Attached filesystem directory for {guest} not found: {local} (resolved to {})",
				dir.display()
			);
		}
		resolved.push((guest, dir));
	}
	match resolved.as_slice() {
		[] => return Ok(None),
		[(guest, dir)] if *guest == FsMounts::ROOT => {
			return Ok(Some(AttachedFs::Dir(dir.clone())));
		}
		_ => {}
	}
	let assembled = TempDir::new().prefix_err(|| "Failed to create a directory for the mounts")?;
	for (guest, dir) in resolved {
		let target = assembled.path().join(guest.trim_start_matches('/'));
		if guest != FsMounts::ROOT && target.exists() {
			anyhow::bail!(
				"The mount at {guest} collides with {guest} inside a mount above it; mount it \
				 elsewhere or remove that directory"
			);
		}
		copy_dir(&dir, &target)?;
	}
	Ok(Some(AttachedFs::Assembled(assembled)))
}

/// Copy the directory `from` into `to`, following symbolic links as packing
/// does.
fn copy_dir(from: &Path, to: &Path) -> Result<()> {
	fs::create_dir_all(to).prefix_err(|| format!("Failed to create {}", to.display()))?;
	for entry in fs::read_dir(from).prefix_err(|| format!("Failed to read {}", from.display()))? {
		let entry = entry.prefix_err(|| format!("Failed to read {}", from.display()))?;
		let source = entry.path();
		let target = to.join(entry.file_name());
		if fs::metadata(&source)
			.prefix_err(|| format!("Failed to read {}", source.display()))?
			.is_dir()
		{
			copy_dir(&source, &target)?;
		} else {
			fs::copy(&source, &target)
				.prefix_err(|| format!("Failed to copy {}", source.display()))?;
		}
	}
	Ok(())
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

#[cfg(test)]
mod tests {
	use super::*;

	fn package_toml(name: &str) -> String {
		format!(
			"abi = 1\n[package]\norganisation = \"test\"\nname = \"{name}\"\nversion = \"1.0.0\"\n"
		)
	}

	#[test]
	fn load_config_package_toml_only() {
		let dir = tempfile::tempdir().unwrap();
		fs::write(dir.path().join(PACKAGE_FILE), package_toml("current")).unwrap();
		let config = load_config(dir.path()).unwrap();
		assert_eq!(config.meta.name, "current");
		assert_eq!(config.abi, AbiVersion::CURRENT);
	}

	#[test]
	fn load_config_surrealism_toml_only() {
		let dir = tempfile::tempdir().unwrap();
		fs::write(
			dir.path().join(surrealism_runtime::config::LEGACY_PACKAGE_FILE),
			package_toml("legacy"),
		)
		.unwrap();
		let config = load_config(dir.path()).unwrap();
		assert_eq!(config.meta.name, "legacy");
	}

	#[test]
	fn load_config_both_prefers_package_toml() {
		let dir = tempfile::tempdir().unwrap();
		fs::write(dir.path().join(PACKAGE_FILE), package_toml("current")).unwrap();
		// Unparseable, so the test fails if `load_config` reads it.
		fs::write(dir.path().join(surrealism_runtime::config::LEGACY_PACKAGE_FILE), "not toml [")
			.unwrap();
		let config = load_config(dir.path()).unwrap();
		assert_eq!(config.meta.name, "current");
	}

	#[test]
	fn mounts_are_assembled_at_their_guest_paths() {
		let dir = tempfile::tempdir().unwrap();
		fs::create_dir_all(dir.path().join("fs/static")).unwrap();
		fs::write(dir.path().join("fs/static/a.txt"), "a").unwrap();
		fs::create_dir_all(dir.path().join("models")).unwrap();
		fs::write(dir.path().join("models/m.bin"), "m").unwrap();
		let mut package = PackageConfig::open({
			fs::write(dir.path().join(PACKAGE_FILE), package_toml("mounts")).unwrap();
			dir.path()
		})
		.unwrap();
		package.add_fs("fs", "/").unwrap();
		let only_root = resolve_attach_fs(dir.path(), package.config()).unwrap().unwrap();
		assert!(matches!(only_root, AttachedFs::Dir(_)));

		package.add_fs("models", "/data/models").unwrap();
		let both = resolve_attach_fs(dir.path(), package.config()).unwrap().unwrap();
		assert_eq!(fs::read_to_string(both.path().join("static/a.txt")).unwrap(), "a");
		assert_eq!(fs::read_to_string(both.path().join("data/models/m.bin")).unwrap(), "m");

		package.add_fs("models", "/static").unwrap();
		let err = resolve_attach_fs(dir.path(), package.config()).err().unwrap();
		assert!(err.to_string().contains("collides"), "{err}");
	}

	#[test]
	fn load_config_neither() {
		let dir = tempfile::tempdir().unwrap();
		let err = load_config(dir.path()).unwrap_err();
		assert!(err.to_string().contains("package.toml"), "{err}");
	}

	#[test]
	fn the_wasm_file_is_named_after_the_library_target() {
		let package = serde_json::json!({
			"targets": [
				{ "kind": ["bin"], "name": "tool" },
				{ "kind": ["cdylib"], "name": "custom_name" },
			]
		});
		assert_eq!(wasm_filename(&package, "my-module"), "custom_name.wasm");
	}

	#[test]
	fn without_a_library_target_the_package_name_is_used() {
		let package = serde_json::json!({ "targets": [{ "kind": ["bin"], "name": "tool" }] });
		assert_eq!(wasm_filename(&package, "my-module"), "my_module.wasm");
		assert_eq!(wasm_filename(&serde_json::json!({}), "my-module"), "my_module.wasm");
	}
}
