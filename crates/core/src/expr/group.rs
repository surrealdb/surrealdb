use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Deref;

use revision::revisioned;

use crate::expr::Expr;
use crate::expr::expression::VisitExpression;
use crate::expr::idiom::Idiom;
use crate::fmt::Fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Groups(pub Vec<Group>);

impl Groups {
	pub(crate) fn is_group_all_only(&self) -> bool {
		self.0.is_empty()
	}
}
impl Deref for Groups {
	type Target = Vec<Group>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Groups {
	type Item = Group;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl VisitExpression for Groups {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.0.iter().for_each(|group| group.visit(visitor));
	}
}

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
pub struct Group(pub Idiom);

impl Deref for Group {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl VisitExpression for Group {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.0.iter().for_each(|part| part.visit(visitor));
	}
}

impl Display for Group {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}
