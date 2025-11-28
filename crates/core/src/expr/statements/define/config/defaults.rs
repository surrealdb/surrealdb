use std::fmt::{self, Display};

use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Expr;
use crate::expr::parameterize::expr_to_ident;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefaultConfig {
	pub namespace: Option<Expr>,
	pub database: Option<Expr>,
}

impl DefaultConfig {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> anyhow::Result<crate::catalog::DefaultConfig> {
		let namespace = if let Some(namespace) = &self.namespace {
			Some(expr_to_ident(stk, ctx, opt, doc, namespace, "namespace").await?)
		} else {
			None
		};

		let database = if let Some(database) = &self.database {
			Some(expr_to_ident(stk, ctx, opt, doc, database, "database").await?)
		} else {
			None
		};

		Ok(crate::catalog::DefaultConfig {
			namespace,
			database,
		})
	}
}

impl Display for DefaultConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " DEFAULT")?;
		if let Some(namespace) = &self.namespace {
			write!(f, " NAMESPACE {}", namespace)?;
		}
		if let Some(database) = &self.database {
			write!(f, " DATABASE {}", database)?;
		}
		Ok(())
	}
}
