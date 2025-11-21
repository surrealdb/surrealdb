use std::fmt;

use crate::fmt::EscapeKwFreeIdent;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Model {
	pub name: String,
	pub version: String,
}

impl fmt::Display for Model {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("ml")?;
		for s in self.name.split("::") {
			f.write_str("::")?;
			EscapeKwFreeIdent(s).fmt(f)?;
		}

		write!(f, "<{}>", self.version)
	}
}

impl From<Model> for crate::expr::Model {
	fn from(v: Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
		}
	}
}
impl From<crate::expr::Model> for Model {
	fn from(v: crate::expr::Model) -> Self {
		Self {
			name: v.name,
			version: v.version,
		}
	}
}
