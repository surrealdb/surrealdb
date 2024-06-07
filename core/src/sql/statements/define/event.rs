use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Ident, Object, Strand, Value, Values};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineEventStatement {
	pub name: Ident,
	pub what: Ident,
	pub when: Value,
	pub then: Values,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineEventStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
		// Claim transaction
		let mut run = ctx.tx_lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if event already exists
		if run.get_tb_event(opt.ns(), opt.db(), &self.what, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::EvAlreadyExists {
					value: self.name.to_string(),
				});
			}
		}
		// Process the statement
		let key = crate::key::table::ev::new(opt.ns(), opt.db(), &self.what, &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		run.add_tb(opt.ns(), opt.db(), &self.what, opt.strict).await?;
		run.set(
			key,
			DefineEventStatement {
				if_not_exists: false,
				..self.clone()
			},
		)
		.await?;
		// Clear the cache
		let key = crate::key::table::ev::prefix(opt.ns(), opt.db(), &self.what);
		run.clr(key).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE EVENT",)?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {} ON {} WHEN {} THEN {}", self.name, self.what, self.when, self.then)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineEventStatement {
	fn structure(self) -> Value {
		let Self {
			name,
			what,
			when,
			then,
			comment,
			..
		} = self;
		let mut acc = Object::default();

		acc.insert("name".to_string(), name.structure());

		acc.insert("what".to_string(), what.structure());

		acc.insert("when".to_string(), when.structure());

		acc.insert(
			"then".to_string(),
			Value::Array(then.0.iter().map(|v| v.to_string().into()).collect()),
		);

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		Value::Object(acc)
	}
}
