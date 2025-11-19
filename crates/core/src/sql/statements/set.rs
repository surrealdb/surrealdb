use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeIdent;
use crate::sql::{Expr, Kind};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SetStatement {
	pub name: String,
	pub what: Expr,
	pub kind: Option<Kind>,
}

impl ToSql for SetStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, "LET ${}", EscapeIdent(&self.name));
		if let Some(ref kind) = self.kind {
			write_sql!(f, ": {}", kind);
		}
		f.push_str(" = ");
		self.what.fmt_sql(f, fmt);
	}
}

impl From<SetStatement> for crate::expr::statements::SetStatement {
	fn from(v: SetStatement) -> Self {
		crate::expr::statements::SetStatement {
			name: v.name,
			what: v.what.into(),
			kind: v.kind.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::SetStatement> for SetStatement {
	fn from(v: crate::expr::statements::SetStatement) -> Self {
		SetStatement {
			name: v.name,
			what: v.what.into(),
			kind: v.kind.map(Into::into),
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::syn;

	#[test]
	fn check_type() {
		let query = syn::parse("LET $param = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param = 5;");

		let query = syn::parse("LET $param: number = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param: number = 5;");
	}
}
