use std::fmt::{self, Display};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::NamespaceDefinition;
use crate::catalog::providers::NamespaceProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineNamespaceStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub comment: Option<Expr>,
}

impl VisitExpression for DefineNamespaceStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.name.visit(visitor);
		self.comment.iter().for_each(|comment| comment.visit(visitor))
	}
}

impl Default for DefineNamespaceStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::String(String::new())),
			comment: None,
		}
	}
}

impl DefineNamespaceStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Process the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "namespace name").await?;

		// Check if the definition exists
		let namespace_id = if let Some(ns) = txn.get_ns_by_name(&name).await? {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::NsAlreadyExists {
							name: name.clone(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
			ns.namespace_id
		} else {
			ctx.try_get_sequences()?.next_namespace_id(Some(ctx)).await?
		};

		// Process the statement
		let ns_def = NamespaceDefinition {
			namespace_id,
			name,
			comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
		};
		txn.put_ns(ns_def).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineNamespaceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE NAMESPACE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", &self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
		}
		Ok(())
	}
}
