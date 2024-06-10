use crate::ctx::Context;
use crate::dbs::{Force, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::UpdateStatement;
use crate::sql::{Base, Ident, Idioms, Index, Object, Output, Part, Strand, Value, Values};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::sync::Arc;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineIndexStatement {
	pub name: Ident,
	pub what: Ident,
	pub cols: Idioms,
	pub index: Index,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		if txn.get_tb_index(opt.ns()?, opt.db()?, &self.what, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::IxAlreadyExists {
					value: self.name.to_string(),
				});
			}
		}
		// Does the table exists?
		match txn.get_tb(opt.ns()?, opt.db()?, &self.what).await {
			Ok(db) => {
				// Are we SchemaFull?
				if db.full {
					// Check that the fields exists
					for idiom in self.cols.iter() {
						if let Some(Part::Field(id)) = idiom.first() {
							txn.get_tb_field(opt.ns()?, opt.db()?, &self.what, id).await?;
						}
					}
				}
			}
			// If the TB was not found, we're fine
			Err(Error::TbNotFound {
				..
			}) => {}
			// Any other error should be returned
			Err(e) => return Err(e),
		}
		// Process the statement
		let key = crate::key::table::ix::new(opt.ns()?, opt.db()?, &self.what, &self.name);
		txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
		txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
		txn.get_or_add_tb(opt.ns()?, opt.db()?, &self.what, opt.strict).await?;
		txn.set(
			key,
			DefineIndexStatement {
				// Don't persist the `IF NOT EXISTS` clause to schema
				if_not_exists: false,
				..self.clone()
			},
		)
		.await?;
		// Clear the cache
		txn.clear();
		// Force queries to run
		let opt = &opt.new_with_force(Force::Index(Arc::new([self.clone()])));
		// Update the index data
		let stm = UpdateStatement {
			what: Values(vec![Value::Table(self.what.clone().into())]),
			output: Some(Output::None),
			..UpdateStatement::default()
		};
		stm.compute(stk, ctx, opt, doc).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineIndexStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE INDEX")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {} ON {} FIELDS {}", self.name, self.what, self.cols)?;
		if Index::Idx != self.index {
			write!(f, " {}", self.index)?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineIndexStatement {
	fn structure(self) -> Value {
		let Self {
			name,
			what,
			cols,
			index,
			comment,
			..
		} = self;
		let mut acc = Object::default();

		acc.insert("name".to_string(), name.structure());

		acc.insert("what".to_string(), what.structure());

		acc.insert("cols".to_string(), cols.structure());

		acc.insert("index".to_string(), index.structure());

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		Value::Object(acc)
	}
}
