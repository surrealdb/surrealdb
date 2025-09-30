use std::fmt::{self, Display, Write};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{FunctionDefinition, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::{Base, Block, Expr, Kind};
use crate::fmt::{EscapeKwFreeIdent, is_pretty, pretty_indent};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineFunctionStatement {
	pub kind: DefineKind,
	pub name: String,
	pub args: Vec<(String, Kind)>,
	pub block: Block,
	pub comment: Option<Expr>,
	pub permissions: Permission,
	pub returns: Option<Kind>,
}

impl VisitExpression for DefineFunctionStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.block.visit(visitor);
		self.comment.iter().for_each(|comment| comment.visit(visitor));
	}
}

impl DefineFunctionStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		if txn.get_db_function(ns, db, &self.name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::FcAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}
		// Process the statement
		{
			let (ns, db) = opt.ns_db()?;
			txn.get_or_add_db(ns, db, opt.strict).await?
		};

		txn.put_db_function(
			ns,
			db,
			&FunctionDefinition {
				name: self.name.clone(),
				args: self.args.clone(),
				block: self.block.clone(),
				comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
				permissions: self.permissions.clone(),
				returns: self.returns.clone(),
			},
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl fmt::Display for DefineFunctionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FUNCTION")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " fn::{}(", &*self.name)?;
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			write!(f, "${}: {kind}", EscapeKwFreeIdent(name))?;
		}
		f.write_str(") ")?;
		if let Some(ref v) = self.returns {
			write!(f, "-> {v} ")?;
		}
		Display::fmt(&self.block, f)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "PERMISSIONS {}", self.permissions)?;
		Ok(())
	}
}
