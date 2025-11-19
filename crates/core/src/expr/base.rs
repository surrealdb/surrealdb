use std::fmt;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum Base {
	#[default]
	Root,
	Ns,
	Db,
}

impl fmt::Display for Base {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Ns => f.write_str("NAMESPACE"),
			Self::Db => f.write_str("DATABASE"),
			Self::Root => f.write_str("ROOT"),
		}
	}
}
