use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::{
	ctx::Context,
	dbs::Options,
	err::Error,
	iam::{Action, ResourceKind},
	sql::{statements::info::InfoStructure, Base, Ident, Kind, Strand, Value},
};

use super::CursorDoc;
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineTypeStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub if_not_exists: bool,
	pub overwrite: bool,
	pub kind: Kind,
	pub comment: Option<Strand>,
}

impl DefineTypeStatement {
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Type, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = (opt.ns()?, opt.db()?);
		// Check if the type already exists
		if txn.get_db_type(ns, db, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::TyAlreadyExists {
					name: self.name.to_string(),
				});
			}
		}

		// Store the type definition
		let key = crate::key::database::ty::new(ns, db, &self.name);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		let ty = DefineTypeStatement {
			// Don't persist the `IF NOT EXISTS` clause to schema
			if_not_exists: false,
			overwrite: false,
			..self.clone()
		};
		txn.set(key, revision::to_vec(&ty)?, None).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineTypeStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TYPE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {} AS {}", self.name, self.kind)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineTypeStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"kind".to_string() => self.kind.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
