use std::fmt::{self, Display};

use surrealdb_types::{SqlFormat, SurrealValue, ToSql};

/// REST API method.
#[derive(SurrealValue, Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[surreal(crate = "surrealdb_types")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[surreal(untagged, lowercase)]
pub enum ApiMethod {
	/// REST DELETE method.
	Delete,
	/// REST GET method.
	#[default]
	Get,
	/// REST PATCH method.
	Patch,
	/// REST POST method.
	Post,
	/// REST PUT method.
	Put,
	/// REST TRACE method.
	Trace,
}

impl Display for ApiMethod {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Delete => write!(f, "delete"),
			Self::Get => write!(f, "get"),
			Self::Patch => write!(f, "patch"),
			Self::Post => write!(f, "post"),
			Self::Put => write!(f, "put"),
			Self::Trace => write!(f, "trace"),
		}
	}
}

impl ToSql for ApiMethod {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_string().fmt_sql(f, fmt)
	}
}

/// Whose permissions the handler of an API runs with.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ApiSecurity {
	/// The handler runs with table and field permissions lifted, bounded by
	/// the auth of the identity that defined the API.
	#[default]
	Definer,
	/// The handler runs under the permissions of the caller, bounded by the
	/// auth of the identity that defined the API.
	Invoker,
}

impl Display for ApiSecurity {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Definer => write!(f, "DEFINER"),
			Self::Invoker => write!(f, "INVOKER"),
		}
	}
}

impl ToSql for ApiSecurity {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(match self {
			Self::Definer => "DEFINER",
			Self::Invoker => "INVOKER",
		});
	}
}
