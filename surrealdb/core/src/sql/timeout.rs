use std::fmt;

use crate::sql::{Expr, Literal};
use crate::types::PublicDuration;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Timeout(pub(crate) Expr);

impl Default for Timeout {
	fn default() -> Self {
		Self(Expr::Literal(Literal::Duration(PublicDuration::default())))
	}
}

impl fmt::Display for Timeout {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "TIMEOUT {}", self.0)
	}
}

impl From<Timeout> for crate::expr::Timeout {
	fn from(v: Timeout) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Timeout> for Timeout {
	fn from(v: crate::expr::Timeout) -> Self {
		Self(v.0.into())
	}
}

impl From<std::time::Duration> for Timeout {
	fn from(v: std::time::Duration) -> Self {
		Self(Expr::Literal(Literal::Duration(PublicDuration::from(v))))
	}
}
