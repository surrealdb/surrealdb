pub use url::{ParseError as UrlParseError, Url};

pub trait IntoUrl {
	fn to_url(self) -> Result<Url, UrlParseError>;
}

impl IntoUrl for String {
	fn to_url(self) -> Result<Url, UrlParseError> {
		self.parse()
	}
}

impl IntoUrl for &str {
	fn to_url(self) -> Result<Url, UrlParseError> {
		self.parse()
	}
}

impl IntoUrl for Url {
	fn to_url(self) -> Result<Url, UrlParseError> {
		Ok(self)
	}
}

impl IntoUrl for &Url {
	fn to_url(self) -> Result<Url, UrlParseError> {
		Ok(self.clone())
	}
}
