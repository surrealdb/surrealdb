use std::error::Error as StdError;

use crate::http::{header::HeaderMap, status::StatusCode};
use url::Url;

use crate::opt::Tls;

use super::{Backend, Client, Error};

pub struct Attempt<'a> {
	pub(super) previous: &'a [Url],
	pub(super) next: &'a Url,
	pub(super) status: StatusCode,
}

impl<'a> Attempt<'a> {
	pub fn previous(&self) -> &[Url] {
		self.previous
	}

	pub fn next(&self) -> &Url {
		self.next
	}

	pub fn status(&self) -> StatusCode {
		self.status
	}
}

pub enum RedirectAction {
	// Follow the redirect,
	Follow,
	// Don't follow the redirect,
	Stop,
	// Return an error,
	Error(Box<dyn StdError + Send + Sync>),
}

impl RedirectAction {
	/// Create an error redirect action
	pub fn error<E>(e: E) -> Self
	where
		E: Into<Box<dyn StdError + Send + Sync>>,
	{
		RedirectAction::Error(e.into())
	}
}

pub enum RedirectPolicy {
	// Limit the redirects to a given number of of redirects..
	Limit(usize),
	// Use the function to determine the result.
	Custom(Box<dyn Fn(Attempt<'_>) -> RedirectAction + Send + Sync + 'static>),
	// Don't follow
	None,
}

impl RedirectPolicy {
	pub fn custom<F>(f: F) -> Self
	where
		F: Fn(Attempt<'_>) -> RedirectAction + Send + Sync + 'static,
	{
		Self::Custom(Box::new(f))
	}
}

pub struct ClientBuilder {
	pub(super) default_headers: Option<HeaderMap>,
	pub(super) tls_config: Option<Tls>,
	pub(super) redirect_policy: RedirectPolicy,
}

impl Default for ClientBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl ClientBuilder {
	pub fn new() -> Self {
		ClientBuilder {
			default_headers: None,
			tls_config: None,
			redirect_policy: RedirectPolicy::Limit(10),
		}
	}

	pub fn with_tls(mut self, tls: Tls) -> Self {
		self.tls_config = Some(tls);
		self
	}

	pub fn default_headers(mut self, headers: HeaderMap) -> Self {
		self.default_headers = Some(headers);
		self
	}

	pub fn redirect(mut self, redirect: RedirectPolicy) -> Self {
		self.redirect_policy = redirect;
		self
	}

	pub fn build(self) -> Result<Client, Error> {
		Ok(Client {
			inner: Backend::build(self),
		})
	}
}
