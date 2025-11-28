use std::fmt::{self, Display};

use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, Literal};
use crate::expr::parameterize::expr_to_ident;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefaultConfig {
	pub namespace: Expr,
	pub database: Expr,
}

impl Default for DefaultConfig {
	fn default() -> Self {
		Self {
			namespace: Expr::Literal(Literal::None),
			database: Expr::Literal(Literal::None),
		}
	}
}

impl DefaultConfig {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> anyhow::Result<crate::catalog::DefaultConfig> {
		let namespace = match &self.namespace {
			Expr::Literal(Literal::None) => None,
			x => Some(expr_to_ident(stk, ctx, opt, doc, x, "namespace").await?),
		};

		let database = match &self.database {
			Expr::Literal(Literal::None) => None,
			x => Some(expr_to_ident(stk, ctx, opt, doc, x, "database").await?),
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
		write!(f, " NAMESPACE {}", self.namespace)?;
		write!(f, " DATABASE {}", self.database)?;
		Ok(())
	}
}
