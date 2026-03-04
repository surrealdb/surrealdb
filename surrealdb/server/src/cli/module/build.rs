use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Result;
use surrealism_runtime::PrefixErr;
use surrealism_runtime::config::{AbiVersion, SurrealismConfig};
use surrealism_runtime::package::{SurrealismPackage, detect_module_kind};
use tempfile::TempDir;
use walrus::Module;
use wasm_opt::OptimizationOptions;

pub async fn init(path: Option<PathBuf>, out: Option<PathBuf>) -> Result<()> {
	let path = path.unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
	let config = load_config(&path)?;
	let source_wasm = get_source_wasm(&path, &config)?;

	build_wasm_module(&path, &config)?;
	let wasm = optimize_wasm(&source_wasm, &config)?;

	// Pack the optimized WASM into a Surrealism package
	let kind = detect_module_kind(&wasm);
	let package = SurrealismPackage {
		config,
		wasm,
		kind,
	};
	let out = resolve_output_path(out, &package.config)?;
	package.pack(out).prefix_err(|| "Failed to pack Surrealism package")?;

	Ok(())
}

fn load_config(path: &Path) -> Result<SurrealismConfig> {
	let surrealism_toml = path.join("surrealism.toml");
	if !surrealism_toml.exists() {
		anyhow::bail!("surrealism.toml not found in the current directory");
	}

	Ok(SurrealismConfig::parse(
		&fs::read_to_string(&surrealism_toml).prefix_err(|| "Failed to read surrealism.toml")?,
	)?)
}

fn wasm_target(config: &SurrealismConfig) -> &'static str {
	match config.abi {
		AbiVersion::P1 => "wasm32-wasip1",
		AbiVersion::P2 => "wasm32-wasip2",
	}
}

fn build_wasm_module(path: &PathBuf, config: &SurrealismConfig) -> Result<()> {
	let target = wasm_target(config);
	println!("Building WASM module (target: {target})...");

	let mut cmd = Command::new("cargo");
	cmd.args(["build", "--target", target, "--release"]);

	if config.abi == AbiVersion::P2 {
		cmd.args(["--features", "p2"]);
	}

	let cargo_status =
		cmd.current_dir(path).status().prefix_err(|| "Failed to execute cargo build")?;

	if !cargo_status.success() {
		anyhow::bail!("Cargo build failed");
	}

	Ok(())
}

fn optimize_wasm(source_wasm: &PathBuf, config: &SurrealismConfig) -> Result<Vec<u8>> {
	if !source_wasm.exists() {
		anyhow::bail!("Expected WASM file not found: {}", source_wasm.display());
	}

	println!("Optimizing bundle...");

	let wasm_bytes = fs::read(source_wasm).prefix_err(|| "Failed to read WASM file")?;

	match config.abi {
		AbiVersion::P1 => {
			let stripped_bytes = strip_wasm_sections(&wasm_bytes)?;
			apply_wasm_opt(&stripped_bytes)
		}
		AbiVersion::P2 => {
			// P2 components have a different binary structure — walrus and wasm-opt
			// operate on core modules and don't support the component model format.
			// For now, return the raw bytes. Future: use wasm-tools component
			// optimisation passes.
			Ok(wasm_bytes)
		}
	}
}

fn strip_wasm_sections(wasm_bytes: &[u8]) -> Result<Vec<u8>> {
	let mut module =
		Module::from_buffer(wasm_bytes).prefix_err(|| "Failed to parse WASM module")?;

	// Strip debug information and other unnecessary sections
	let mut sections_to_remove = Vec::new();
	for (id, custom) in module.customs.iter() {
		let name = custom.name();
		if name.starts_with(".debug")
			|| name == "name"
			|| name == "sourceMappingURL"
			|| name.starts_with("reloc.")
			|| name.starts_with("linking")
			|| name == "target_features"
		{
			sections_to_remove.push(id);
		}
	}
	for id in sections_to_remove {
		module.customs.delete(id);
	}

	// Clear producers section
	module.producers.clear();

	Ok(module.emit_wasm())
}

fn apply_wasm_opt(wasm_bytes: &[u8]) -> Result<Vec<u8>> {
	let mut opts = OptimizationOptions::new_optimize_for_size_aggressively();
	opts.enable_feature(wasm_opt::Feature::BulkMemory);
	opts.enable_feature(wasm_opt::Feature::Simd);
	opts.enable_feature(wasm_opt::Feature::Atomics);
	opts.enable_feature(wasm_opt::Feature::MutableGlobals);
	opts.enable_feature(wasm_opt::Feature::TruncSat);
	opts.enable_feature(wasm_opt::Feature::SignExt);
	opts.debug_info(false);

	// Create a temporary directory for wasm-opt files
	let temp_dir = TempDir::new().prefix_err(|| "Failed to create temporary directory")?;
	let temp_wasm_input = temp_dir.path().join("input.wasm");
	let temp_wasm_output = temp_dir.path().join("output.wasm");

	fs::write(&temp_wasm_input, wasm_bytes).prefix_err(|| "Failed to write temporary WASM file")?;

	opts.run(&temp_wasm_input, &temp_wasm_output)
		.prefix_err(|| "Failed to optimize WASM with wasm-opt")?;

	Ok(fs::read(&temp_wasm_output).prefix_err(|| "Failed to read optimized WASM file")?)
}

fn get_source_wasm(path: &PathBuf, config: &SurrealismConfig) -> Result<PathBuf> {
	let metadata = metadata(path).prefix_err(|| "Failed to retrieve cargo metadata")?;

	let target_directory = metadata["target_directory"]
		.as_str()
		.ok_or_else(|| anyhow::anyhow!("No target_directory found in cargo metadata"))?;

	// Find the package name from metadata
	let packages = metadata["packages"]
		.as_array()
		.ok_or_else(|| anyhow::anyhow!("No packages found in cargo metadata"))?;

	// Find the current package (the one in the specified directory)
	let current_package = packages
		.iter()
		.find(|pkg| {
			let manifest_path = pkg["manifest_path"].as_str().unwrap_or("");
			let manifest_pathbuf = PathBuf::from(manifest_path);
			let manifest_dir = manifest_pathbuf.parent();

			// Canonicalize both paths for comparison to handle relative vs absolute paths
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
	let target = wasm_target(config);
	let target_dir = PathBuf::from(target_directory).join(format!("{target}/release"));
	Ok(target_dir.join(&wasm_filename))
}

fn metadata(path: &PathBuf) -> Result<serde_json::Value> {
	let output = Command::new("cargo")
		.args(["metadata", "--format-version", "1", "--no-deps"])
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

fn resolve_output_path(out: Option<PathBuf>, config: &SurrealismConfig) -> Result<PathBuf> {
	match out {
		None => {
			// No output specified, use default filename in current working directory
			Ok(std::env::current_dir().unwrap_or_default().join(config.file_name()))
		}
		Some(out_path) => {
			if out_path.is_absolute() {
				// Absolute path, use as-is
				Ok(out_path)
			} else {
				// Relative path (including just filenames), resolve relative to current working
				// directory
				Ok(std::env::current_dir().unwrap_or_default().join(out_path))
			}
		}
	}
}
