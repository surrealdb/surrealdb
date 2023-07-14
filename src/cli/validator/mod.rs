#[cfg(feature = "has-storage")]
use std::{
	path::{Path, PathBuf},
	str::FromStr,
	time::Duration,
};

pub(crate) mod parser;

#[cfg(feature = "has-storage")]
pub(crate) fn path_valid(v: &str) -> Result<String, String> {
	match v {
		"memory" => Ok(v.to_string()),
		v if v.starts_with("file:") => Ok(v.to_string()),
		v if v.starts_with("rocksdb:") => Ok(v.to_string()),
		v if v.starts_with("speedb:") => Ok(v.to_string()),
		v if v.starts_with("tikv:") => Ok(v.to_string()),
		v if v.starts_with("fdb:") => Ok(v.to_string()),
		_ => Err(String::from("Provide a valid database path parameter")),
	}
}

#[cfg(feature = "has-storage")]
pub(crate) fn file_exists(path: &str) -> Result<PathBuf, String> {
	let path = Path::new(path);
	if !*path.try_exists().as_ref().map_err(ToString::to_string)? {
		return Err(String::from("Ensure the file exists"));
	}
	if !path.is_file() {
		return Err(String::from("Ensure the path is a file"));
	}
	Ok(path.to_owned())
}

pub(crate) fn endpoint_valid(v: &str) -> Result<String, String> {
	fn split_endpoint(v: &str) -> (&str, &str) {
		match v {
			"memory" => ("mem", ""),
			v => match v.split_once("://") {
				Some(parts) => parts,
				None => v.split_once(':').unwrap_or_default(),
			},
		}
	}

	let scheme = split_endpoint(v).0;
	match scheme {
		"http" | "https" | "ws" | "wss" | "fdb" | "mem" | "rocksdb" | "file" | "tikv" => {
			Ok(v.to_string())
		}
		_ => Err(String::from("Provide a valid database connection string")),
	}
}

pub(crate) fn into_valid(v: &str) -> Result<String, String> {
	match v {
		v if v.ends_with(".db") => Ok(v.to_string()),
		v if v.starts_with("http://") => Ok(v.to_string()),
		v if v.starts_with("https://") => Ok(v.to_string()),
		"-" => Ok(v.to_string()),
		_ => Err(String::from("Provide a valid database connection string, or the path to a file")),
	}
}

#[cfg(feature = "has-storage")]
pub(crate) fn key_valid(v: &str) -> Result<String, String> {
	match v.len() {
		16 => Ok(v.to_string()),
		24 => Ok(v.to_string()),
		32 => Ok(v.to_string()),
		_ => Err(String::from("Ensure your database encryption key is 16, 24, or 32 bytes long")),
	}
}

#[cfg(feature = "has-storage")]
pub(crate) fn duration(v: &str) -> Result<Duration, String> {
	surrealdb::sql::Duration::from_str(v).map(|d| d.0).map_err(|_| String::from("invalid duration"))
}
