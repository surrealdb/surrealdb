use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

use anyhow::Result;
use surrealism_types::err::PrefixError;
use tar::Archive;
use zstd::stream::read::Decoder;

use crate::config::SurrealismConfig;

pub struct SurrealismPackage {
	pub config: SurrealismConfig,
	pub wasm: Vec<u8>,
}

impl SurrealismPackage {
	pub fn from_file(file: PathBuf) -> Result<Self> {
		// Check if the file extension is .surli
		if file.extension().and_then(|s| s.to_str()) != Some("surli") {
			anyhow::bail!("Only .surli files are supported");
		}

		// Check if the file exists
		if !file.exists() {
			anyhow::bail!("File not found: {}", file.display());
		}

		// Unpack the .tar.zst file in memory
		let archive_file = File::open(file).prefix_err(|| "Failed to open archive file")?;
		SurrealismPackage::from_reader(archive_file)
	}

	pub fn from_reader<R: Read>(reader: R) -> Result<Self> {
		let zstd_decoder =
			Decoder::new(BufReader::new(reader)).prefix_err(|| "Failed to create zstd decoder")?;
		let mut archive = Archive::new(zstd_decoder);

		// Placeholders for the WASM and config
		let mut wasm: Option<Vec<u8>> = None;
		let mut config: Option<SurrealismConfig> = None;

		// Extract files in memory
		for entry in archive.entries().prefix_err(|| "Failed to read archive entries")? {
			let mut entry = entry.prefix_err(|| "Failed to read archive entry")?;
			let path = entry.path().prefix_err(|| "Failed to get entry path")?;

			match path.to_string_lossy() {
				path if path.ends_with("mod.wasm") => {
					// Look for the mod.wasm file
					let mut buffer = Vec::new();
					entry
						.read_to_end(&mut buffer)
						.prefix_err(|| "Failed to read WASM file from archive")?;
					wasm = Some(buffer);
				}
				path if path.ends_with("surrealism.toml") => {
					// Look for the surrealism.toml file
					let mut buffer = String::new();
					entry
						.read_to_string(&mut buffer)
						.prefix_err(|| "Failed to read config file from archive")?;
					config = Some(
						SurrealismConfig::parse(&buffer)
							.prefix_err(|| "Failed to parse surrealism.toml")?,
					);
				}
				_ => {
					// Ignore other files
					continue;
				}
			}

			if wasm.is_some() && config.is_some() {
				// If both files are found, we can stop reading further
				break;
			}
		}

		let wasm = wasm.ok_or_else(|| anyhow::anyhow!("mod.wasm not found in archive"))?;
		let config =
			config.ok_or_else(|| anyhow::anyhow!("surrealism.toml not found in archive"))?;

		Ok(SurrealismPackage {
			config,
			wasm,
		})
	}

	pub fn pack(&self, output: PathBuf) -> Result<()> {
		// Check if the output file has the correct extension
		if output.extension().and_then(|s| s.to_str()) != Some("surli") {
			anyhow::bail!("Output file must have .surli extension");
		}

		// Create a new tar.zst archive
		let file = File::create(&output).prefix_err(|| "Failed to create output file")?;
		let encoder =
			zstd::stream::Encoder::new(file, 0).prefix_err(|| "Failed to create zstd encoder")?;
		let mut archive = tar::Builder::new(encoder);

		// Add the WASM file
		let mut wasm_reader = std::io::Cursor::new(&self.wasm);
		let mut wasm_header = tar::Header::new_gnu();
		wasm_header.set_size(self.wasm.len() as u64);
		archive
			.append_data(&mut wasm_header, "surrealism/mod.wasm", &mut wasm_reader)
			.prefix_err(|| "Failed to add mod.wasm to archive")?;

		// Add the config file
		let config_str = self.config.to_string().prefix_err(|| "Failed to serialize config")?;
		let config_bytes = config_str.as_bytes();
		let mut config_reader = std::io::Cursor::new(config_bytes);
		let mut config_header = tar::Header::new_gnu();
		config_header.set_size(config_bytes.len() as u64);
		archive
			.append_data(&mut config_header, "surrealism/surrealism.toml", &mut config_reader)
			.prefix_err(|| "Failed to add surrealism.toml to archive")?;

		// Finish the archive
		let encoder = archive.into_inner().prefix_err(|| "Failed to get encoder from archive")?;
		encoder.finish().prefix_err(|| "Failed to finish zstd encoder")?;

		Ok(())
	}
}
