#![cfg_attr(docsrs, doc(cfg(not(target_family = "wasm"))))]

use std::path::Path;
use std::path::PathBuf;

#[derive(Debug)]
#[non_exhaustive]
pub enum ExportDestination {
	File(PathBuf),
	Memory,
}

/// A trait for converting inputs into database export locations
pub trait IntoExportDestination<R>: private::Sealed<R> {}

impl<T> IntoExportDestination<PathBuf> for T where T: AsRef<Path> {}
impl<T> private::Sealed<PathBuf> for T
where
	T: AsRef<Path>,
{
	fn into_export_destination(self) -> PathBuf {
		self.as_ref().to_path_buf()
	}
}

impl IntoExportDestination<()> for () {}
impl private::Sealed<()> for () {
	fn into_export_destination(self) {}
}

mod private {
	pub trait Sealed<R> {
		/// Converts an input into a database export location
		fn into_export_destination(self) -> R;
	}
}
