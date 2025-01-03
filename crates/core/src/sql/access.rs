use crate::sql::{
	escape::escape_ident, fmt::Fmt, strand::no_nul_bytes, Duration, Id, Ident, Thing,
};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
// Durations representing the expiration of different elements of the access method
// In this context, the None variant represents that the element does not expire
pub struct AccessDuration {
	// Duration after which the grants generated with the access method expire
	// For access methods whose grants are tokens, this value is irrelevant
	pub grant: Option<Duration>,
	// Duration after which the tokens obtained with the access method expire
	// For access methods that cannot issue tokens, this value is irrelevant
	pub token: Option<Duration>,
	// Duration after which the session authenticated with the access method expires
	pub session: Option<Duration>,
}

impl Default for AccessDuration {
	fn default() -> Self {
		Self {
			// By default, access grants expire in 30 days.
			grant: Some(Duration::from_days(30)),
			// By default, tokens expire after one hour
			token: Some(Duration::from_hours(1)),
			// By default, sessions do not expire
			session: None,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Accesses(pub Vec<Access>);

impl From<Access> for Accesses {
	fn from(v: Access) -> Self {
		Accesses(vec![v])
	}
}

impl Deref for Accesses {
	type Target = Vec<Access>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Accesses {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Access")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Access(#[serde(with = "no_nul_bytes")] pub String);

impl From<String> for Access {
	fn from(v: String) -> Self {
		Self(v)
	}
}

impl From<&str> for Access {
	fn from(v: &str) -> Self {
		Self::from(String::from(v))
	}
}

impl From<Ident> for Access {
	fn from(v: Ident) -> Self {
		Self(v.0)
	}
}

impl Deref for Access {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Access {
	pub fn generate(&self) -> Thing {
		Thing {
			tb: self.0.to_owned(),
			id: Id::rand(),
		}
	}
}

impl Display for Access {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&escape_ident(&self.0), f)
	}
}
