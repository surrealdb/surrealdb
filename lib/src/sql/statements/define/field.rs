use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::DefineTableStatement;
use crate::sql::{
	fmt::is_pretty, fmt::pretty_indent, Base, Ident, Idiom, Kind, Permissions, Strand, Value,
};
use crate::sql::{Relation, TableType};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	pub flex: bool,
	pub kind: Option<Kind>,
	pub value: Option<Value>,
	pub assert: Option<Value>,
	pub default: Option<Value>,
	pub permissions: Permissions,
	pub comment: Option<Strand>,
}

impl DefineFieldStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Process the statement
		let fd = self.name.to_string();
		let key = crate::key::table::fd::new(opt.ns(), opt.db(), &self.what, &fd);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		let tb = run.add_tb(opt.ns(), opt.db(), &self.what, opt.strict).await?;
		run.set(key, self).await?;

		let new_tb =
			match (self.name.to_string().as_str(), tb.table_type.clone(), self.kind.clone()) {
				("in", TableType::Relation(rel), Some(dk)) => {
					if !matches!(dk, Kind::Record(_)) {
						return Err(Error::Thrown(
							"in field on a relation must be a record".into(),
						));
					};
					if rel.from.as_ref() != Some(&dk) {
						Some(DefineTableStatement {
							table_type: TableType::Relation(Relation {
								from: Some(dk),
								..rel
							}),
							..tb
						})
					} else {
						None
					}
				}
				("out", TableType::Relation(rel), Some(dk)) => {
					if !matches!(dk, Kind::Record(_)) {
						return Err(Error::Thrown(
							"out field on a relation must be a record".into(),
						));
					};
					if rel.to.as_ref() != Some(&dk) {
						Some(DefineTableStatement {
							table_type: TableType::Relation(Relation {
								to: Some(dk),
								..rel
							}),
							..tb
						})
					} else {
						None
					}
				}
				_ => None,
			};
		if let Some(tb) = new_tb {
			let key = crate::key::database::tb::new(opt.ns(), opt.db(), &self.what);
			run.set(key, &tb).await?;
			let key = crate::key::table::ft::prefix(opt.ns(), opt.db(), &self.what);
			run.clr(key).await?;
		}

		// Clear the cache
		let key = crate::key::table::fd::prefix(opt.ns(), opt.db(), &self.what);
		run.clr(key).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineFieldStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE FIELD {} ON {}", self.name, self.what)?;
		if self.flex {
			write!(f, " FLEXIBLE")?
		}
		if let Some(ref v) = self.kind {
			write!(f, " TYPE {v}")?
		}
		if let Some(ref v) = self.default {
			write!(f, " DEFAULT {v}")?
		}
		if let Some(ref v) = self.value {
			write!(f, " VALUE {v}")?
		}
		if let Some(ref v) = self.assert {
			write!(f, " ASSERT {v}")?
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "{}", self.permissions)?;
		Ok(())
	}
}
