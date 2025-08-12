use std::borrow::Cow;
#[cfg(test)]
use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Result, bail};
use reqwest::Client;

use crate::cli::upgrade::ROOT;

pub(crate) trait VersionClient {
	async fn fetch(&self, version: &str) -> Result<Cow<'static, str>>;
}

pub(crate) struct ReqwestVersionClient {
	client: Client,
}

pub(crate) fn new(timeout: Option<Duration>) -> Result<ReqwestVersionClient> {
	let mut client = Client::builder();
	if let Some(timeout) = timeout {
		client = client.timeout(timeout);
	}
	let client = client.build()?;
	Ok(ReqwestVersionClient {
		client,
	})
}

impl VersionClient for ReqwestVersionClient {
	async fn fetch(&self, version: &str) -> Result<Cow<'static, str>> {
		let request = self.client.get(format!("{ROOT}/{version}.txt")).build().unwrap();
		let response = self.client.execute(request).await?;
		if !response.status().is_success() {
			bail!("received status {} when fetchin version", response.status())
		}
		Ok(Cow::Owned(response.text().await?.trim().to_owned()))
	}
}

#[cfg(test)]
pub(crate) struct MapVersionClient {
	pub(crate) fetch_mock: BTreeMap<String, fn() -> Result<String>>,
}

#[cfg(test)]
impl VersionClient for MapVersionClient {
	async fn fetch(&self, version: &str) -> Result<Cow<'static, str>> {
		let found = self.fetch_mock[version];
		found().map(Cow::Owned)
	}
}
