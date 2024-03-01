use crate::err::Error;
use pprof::Error::IoError;
use reqwest::Client;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::io::ErrorKind;
use std::time::Duration;

pub(crate) trait VersionClient {
	async fn fetch(&self, version: &str) -> Result<Cow<'_, str>, Error>;
}

struct ReqwestVersionClient {
	client: Client,
}

pub(crate) fn new(timeout: Option<Duration>) -> Result<ReqwestVersionClient, Error> {
	let mut client = Client::builder();
	if let Some(timeout) = timeout {
		client = client.timeout(timeout);
	}
	let client = client.build()?;
	Ok(ReqwestVersionClient {
		client,
	})
}

// impl ReqwestVersionClient {
// 	fn bla(&self)  {
// 		let a =self.fetch("str");
// 		let a = Box::new(a);
// 		return a;
// 	}
// }

impl VersionClient for ReqwestVersionClient {
	async fn fetch(&self, version: &str) -> Result<Cow<'_, str>, Error> {
		let request = self.client.get(format!("{ROOT}/{version}.txt")).build().unwrap();
		let response = self.client.execute(request).await?;
		if !response.status().is_success() {
			return Err(Error::Io(IoError::new(
				ErrorKind::Other,
				format!("received status {} when fetching version", response.status()),
			)));
		}
		Ok(Cow::Owned(response.text().await?.trim().to_owned()))
	}
}

#[cfg(test)]
pub(crate) struct MapVersionClient {
	pub(crate) fetch_mock: BTreeMap<String, fn() -> Result<String, Error>>,
}

#[cfg(test)]
impl VersionClient for MapVersionClient {
	async fn fetch(&self, version: &str) -> Result<Cow<'_, str>, Error> {
		let found = self.fetch_mock.get(version).unwrap();
		match found {
			Ok(s) => Ok(Cow::Borrowed(s)),
			Err(e) => return Err((*e).clone()),
		}
	}
}
