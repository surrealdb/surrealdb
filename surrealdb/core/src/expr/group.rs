use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Deref;

use revision::revisioned;

use crate::expr::idiom::Idiom;
use crate::fmt::Fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Groups(pub(crate) Vec<Group>);

impl Groups {
	pub(crate) fn is_group_all_only(&self) -> bool {
		self.0.is_empty()
	}

	pub(crate) fn len(&self) -> usize {
		self.0.len()
	}
}

// Note: IntoIterator trait intentionally not implemented to avoid exposing private Group type

impl Display for Groups {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if self.0.is_empty() {
			write!(f, "GROUP ALL")
		} else {
			write!(f, "GROUP BY {}", Fmt::comma_separated(&self.0))
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Group(pub(crate) Idiom);

impl Deref for Group {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Group {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}
