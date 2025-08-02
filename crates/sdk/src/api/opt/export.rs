#![cfg_attr(docsrs, doc(cfg(not(target_family = "wasm"))))]

use std::path::{Path, PathBuf};

#[derive(Debug)]
#[non_exhaustive]
pub enum ExportDestination {
	File(PathBuf),
	Memory,
}

/// A trait for converting inputs into database export locations
pub trait IntoExportDestination<R>: into_export_destination::Sealed<R> {}

impl<T> IntoExportDestination<PathBuf> for T where T: AsRef<Path> {}
impl<T> into_export_destination::Sealed<PathBuf> for T
where
	T: AsRef<Path>,
{
	fn into_export_destination(self) -> PathBuf {
		self.as_ref().to_path_buf()
	}
}

impl IntoExportDestination<()> for () {}
impl into_export_destination::Sealed<()> for () {
	fn into_export_destination(self) {}
}

mod into_export_destination {
	pub trait Sealed<R> {
		/// Converts an input into a database export location
		fn into_export_destination(self) -> R;
	}
}
