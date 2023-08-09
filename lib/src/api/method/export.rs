use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::Connection;
use crate::api::Error;
use crate::api::ExtraFeatures;
use crate::api::Result;
use channel::Sender;
use std::borrow::Cow;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::future::Future;
use std::future::IntoFuture;
use std::path::Component;
use std::path::Components;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;

/// A database export future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Export<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) target: Exportable,
}

impl<'r, Client> IntoFuture for Export<'r, Client>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async {
			let router = self.router?;
			if !router.features.contains(&ExtraFeatures::Backup) {
				return Err(Error::BackupsNotSupported.into());
			}
			let mut conn = Client::new(Method::Export);
			match self.target {
				Exportable::File(f) => conn.execute_unit(router, Param::file(f)).await,
				Exportable::Send(s) => conn.execute_unit(router, Param::send(s)).await,
			}
		})
	}
}

#[derive(Debug)]
pub enum Exportable {
	File(PathBuf),
	Send(Sender<Vec<u8>>),
}

pub trait IntoExportable {
	fn into_exportable(self) -> Exportable;
}

impl IntoExportable for &str {
	fn into_exportable(self) -> Exportable {
		Exportable::File(<str as AsRef<Path>>::as_ref(self).to_owned())
	}
}

impl IntoExportable for String {
	fn into_exportable(self) -> Exportable {
		Exportable::File(<str as AsRef<Path>>::as_ref(&self).to_owned())
	}
}

impl IntoExportable for &String {
	fn into_exportable(self) -> Exportable {
		Exportable::File(<str as AsRef<Path>>::as_ref(self).to_owned())
	}
}

impl IntoExportable for &Path {
	fn into_exportable(self) -> Exportable {
		Exportable::File((*self).to_owned())
	}
}

impl IntoExportable for &PathBuf {
	fn into_exportable(self) -> Exportable {
		Exportable::File((*self).to_owned())
	}
}

impl IntoExportable for PathBuf {
	fn into_exportable(self) -> Exportable {
		Exportable::File(self.to_owned())
	}
}

impl IntoExportable for Component<'_> {
	fn into_exportable(self) -> Exportable {
		<Component as AsRef<Path>>::as_ref(&self).into_exportable()
	}
}
impl IntoExportable for Components<'_> {
	fn into_exportable(self) -> Exportable {
		<Components<'_> as AsRef<Path>>::as_ref(&self).into_exportable()
	}
}
impl IntoExportable for Cow<'_, OsStr> {
	fn into_exportable(self) -> Exportable {
		<Cow<'_, OsStr> as AsRef<Path>>::as_ref(&self).into_exportable()
	}
}
impl IntoExportable for std::path::Iter<'_> {
	fn into_exportable(self) -> Exportable {
		self.as_path().into_exportable()
	}
}
impl IntoExportable for OsString {
	fn into_exportable(self) -> Exportable {
		<OsString as AsRef<Path>>::as_ref(&self).into_exportable()
	}
}
impl IntoExportable for Sender<Vec<u8>> {
	fn into_exportable(self) -> Exportable {
		Exportable::Send(self)
	}
}
