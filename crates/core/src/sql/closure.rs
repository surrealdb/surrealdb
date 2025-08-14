use std::fmt;

use crate::sql::{Expr, Ident, Kind};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Closure {
	pub args: Vec<(Ident, Kind)>,
	pub returns: Option<Kind>,
	pub body: Expr,
}

impl fmt::Display for Closure {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("|")?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "${name}: ")?;
			match kind {
				k @ Kind::Either(_) => write!(f, "<{}>", k)?,
				k => write!(f, "{}", k)?,
			}
		}
		f.write_str("|")?;
		if let Some(returns) = &self.returns {
			write!(f, " -> {returns}")?;
		}
		write!(f, " {}", self.body)
	}
}

impl From<Closure> for crate::val::Closure {
	fn from(v: Closure) -> Self {
		Self {
			args: v.args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
			returns: v.returns.map(Into::into),
			body: v.body.into(),
		}
	}
}

impl From<crate::val::Closure> for Closure {
	fn from(v: crate::val::Closure) -> Self {
		Self {
			args: v.args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
			returns: v.returns.map(Into::into),
			body: v.body.into(),
		}
	}
}
