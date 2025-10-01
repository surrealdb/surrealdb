use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub struct Script(pub String);

impl From<String> for Script {
	fn from(s: String) -> Self {
		Self(s)
	}
}

impl From<&str> for Script {
	fn from(s: &str) -> Self {
		Self::from(String::from(s))
	}
}

impl Deref for Script {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Script {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}
