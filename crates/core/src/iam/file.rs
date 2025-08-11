use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use path_clean::PathClean;

use crate::cnf::FILE_ALLOWLIST;
use crate::err::Error;

pub(crate) fn is_path_allowed(path: &Path) -> Result<PathBuf> {
	check_is_path_allowed(path, &FILE_ALLOWLIST)
}

/// Checks if the requested file path is within any of the allowed directories.
fn check_is_path_allowed(path: &Path, allowed_path: &[PathBuf]) -> Result<PathBuf> {
	// Convert the requested path to its canonical form.
	let canonical_path = fs::canonicalize(path)?;

	// If the list is empty, we don't operate any control
	if allowed_path.is_empty() {
		return Ok(canonical_path);
	}

	// Check if the canonical path starts with any of the allowed paths.
	if allowed_path.iter().any(|allowed| canonical_path.starts_with(allowed)) {
		Ok(canonical_path)
	} else {
		Err(anyhow::Error::new(Error::FileAccessDenied(path.to_string_lossy().to_string())))
	}
}

pub(crate) fn extract_allowed_paths(
	input: &str,
	canonicalize: bool,
	subject: &str,
) -> Vec<PathBuf> {
	// or a semicolon on Windows.
	let delimiter = if cfg!(target_os = "windows") {
		";"
	} else {
		":"
	};
	// Split the allowlist string, canonicalize each path, and collect valid paths.
	input
		.split(delimiter)
		.filter_map(|s| {
			let trimmed = s.trim();
			if trimmed.is_empty() {
				None
			} else {
				let path = PathBuf::from(trimmed).clean();
				let path = if canonicalize {
					let Ok(path) = fs::canonicalize(&path) else {
						warn!("Failed to canonicalize {subject} path: {}", path.to_string_lossy());
						return None;
					};

					path
				} else {
					path
				};

				debug!("Allowed {subject} path: {}", path.to_string_lossy());
				Some(path)
			}
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;

	use super::*;

	#[test]
	fn test_empty_allow_list_allows_access() {
		// Create a temporary file in a temp directory.
		let dir = tempdir().expect("failed to create temp dir");
		let file_path = dir.path().join("test.txt");
		fs::write(&file_path, "content").expect("failed to write file in file_path");

		// With an empty allowlist, access should be allowed.
		let result = check_is_path_allowed(&file_path, &[]);
		assert!(result.is_ok(), "File access should be allowed when no restrictions are set");
	}

	#[test]
	fn test_allow_list_access() {
		// Use the appropriate delimiter for the platform.
		let delimiter = if cfg!(target_os = "windows") {
			";"
		} else {
			":"
		};

		// Create 3 temporary directories.
		let (dir1, dir2, dir3) = (tempdir().unwrap(), tempdir().unwrap(), tempdir().unwrap());
		// First two directories are allowed
		let combined = format!(
			"{}{}{}",
			dir1.path().to_string_lossy(),
			delimiter,
			dir2.path().to_string_lossy()
		);
		let allowlist = extract_allowed_paths(&combined, true, "file");

		// Create a file in the first allowed directory.
		let allowed_file1 = dir1.path().join("file1.txt");
		fs::write(&allowed_file1, "content").expect("failed to write file in allowed_file1");

		// Create a file in the second allowed directory.
		let allowed_file2 = dir2.path().join("file2.txt");
		fs::write(&allowed_file2, "content").expect("failed to write file in allowed_file2");

		// Create a file in the third denied directory.
		let denied_file3 = dir3.path().join("file3.txt");
		fs::write(&denied_file3, "content").expect("failed to write file in denied_file3");

		// Check that files in allowed directories are permitted.
		let res1 = check_is_path_allowed(&allowed_file1, &allowlist);
		let res2 = check_is_path_allowed(&allowed_file2, &allowlist);
		assert!(res1.is_ok(), "File in the first allowed directory should be permitted");
		assert!(res2.is_ok(), "File in the second allowed directory should be permitted");

		// Check that the file outside is denied.
		let res_outside = check_is_path_allowed(&denied_file3, &allowlist);
		assert!(res_outside.is_err(), "File outside allowed directories should be denied");
	}
}
