use std::fmt::{self, Display};

use crate::sql::bytesize::Bytesize;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Object, Permission, Timeout, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ApiConfig {
	// Do we want to tackle trusted proxies and ip allow/deny listing at this level?
	pub permissions: Option<Permission>,
	pub timeout: Option<Timeout>,
	pub max_body_size: Option<Bytesize>,
	pub headers: Option<Object>,
}

impl ApiConfig {
	pub fn merge(&self, other: &Self) -> Self {
		Self {
			permissions: other.permissions.clone().or(self.permissions.clone()),
			timeout: other.timeout.clone().or(self.timeout.clone()),
			max_body_size: other.max_body_size.clone().or(self.max_body_size.clone()),
			headers: other.headers.clone().or(self.headers.clone()),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.permissions.is_none() && self.timeout.is_none() && self.max_body_size.is_none() && self.headers.is_none()
	}
}

impl From<MergedApiConfig<'_>> for ApiConfig {
	fn from(m: MergedApiConfig) -> Self {
		Self {
			permissions: m.permissions().cloned(),
			timeout: m.timeout().cloned(),
			max_body_size: m.max_body_size().cloned(),
			headers: m.headers().cloned(),
		}
	}
}

impl Display for ApiConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " API")?;

		if let Some(p) = &self.permissions {
			write!(f, " PERMISSIONS {}", p)?;
		}

		if let Some(t) = &self.timeout {
			write!(f, " {}", t)?;
		}

		if let Some(m) = &self.max_body_size {
			write!(f, " MAX_BODY_SIZE {}", m)?;
		}

		if let Some(h) = &self.headers {
			write!(f, " HEADERS {}", h)?;
		}
		Ok(())
	}
}

impl InfoStructure for ApiConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"permissions", if let Some(v) = self.permissions => v.structure(),
			"timeout", if let Some(v) = self.timeout => v.structure(),
			"max_body_size", if let Some(v) = self.max_body_size => v.structure(),
			"headers", if let Some(v) = self.headers => Value::Object(v),
		))
	}
}

pub struct MergedApiConfig<'a> {
	pub global: Option<&'a ApiConfig>,
	pub stmt: Option<&'a ApiConfig>,
	pub method: Option<&'a ApiConfig>,
}

macro_rules! pick {
	($self:ident, $prop:ident) => {
		if let Some(ApiConfig { $prop: Some(v), .. }) = $self.method {
			Some(v)
		} else if let Some(ApiConfig { $prop: Some(v), .. }) = $self.stmt {
			Some(v)
		} else if let Some(ApiConfig { $prop: Some(v), .. }) = $self.global {
			Some(v)
		} else {
			None
		}
	};
}

impl<'a> MergedApiConfig<'a> {
	pub fn permissions(&self) -> Option<&Permission> {
		pick!(self, permissions)
	}

	pub fn timeout(&self) -> Option<&Timeout> {
		pick!(self, timeout)
	}

	pub fn max_body_size(&self) -> Option<&Bytesize> {
		pick!(self, max_body_size)
	}

	pub fn headers(&self) -> Option<&Object> {
		pick!(self, headers)
	}
}