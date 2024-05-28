use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Ident, Object, Permission, Strand, Value};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineParamStatement {
	pub name: Ident,
	pub value: Value,
	pub comment: Option<Strand>,
	pub permissions: Permission,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineParamStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Parameter, &Base::Db)?;
		// Claim transaction
		let mut run = ctx.tx_lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if param already exists
		if self.if_not_exists && run.get_db_param(opt.ns(), opt.db(), &self.name).await.is_ok() {
			return Err(Error::PaAlreadyExists {
				value: self.name.to_string(),
			});
		}
		// Process the statement
		let key = crate::key::database::pa::new(opt.ns(), opt.db(), &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		run.set(
			key,
			DefineParamStatement {
				// Compute the param
				value: self.value.compute(stk, ctx, opt, doc).await?,
				// Don't persist the "IF NOT EXISTS" clause to schema
				if_not_exists: false,
				..self.clone()
			},
		)
		.await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineParamStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE PARAM")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " ${} VALUE {}", self.name, self.value)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
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

impl InfoStructure for DefineParamStatement {
	fn structure(self) -> Value {
		let Self {
			name,
			value,
			comment,
			permissions,
			..
		} = self;
		let mut acc = Object::default();

		acc.insert("name".to_string(), name.structure());

		acc.insert("value".to_string(), value.structure());

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		acc.insert("permissions".to_string(), permissions.structure());

		Value::Object(acc)
	}
}
