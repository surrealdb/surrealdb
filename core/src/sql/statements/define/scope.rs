use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Duration, Ident, Strand, Value};
use derive::Store;
use rand::distributions::Alphanumeric;
use rand::Rng;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
#[non_exhaustive]
pub struct DefineScopeStatement {
	pub name: Ident,
	pub code: String,
	pub session: Option<Duration>,
	pub signup: Option<Value>,
	pub signin: Option<Value>,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineScopeStatement {
	pub(crate) fn random_code() -> String {
		rand::thread_rng().sample_iter(&Alphanumeric).take(128).map(char::from).collect::<String>()
	}
}

impl DefineScopeStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Scope, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if scope already exists
		if self.if_not_exists && run.get_sc(opt.ns(), opt.db(), &self.name).await.is_ok() {
			return Err(Error::ScAlreadyExists {
				value: self.name.to_string(),
			});
		}
		// Process the statement
		let key = crate::key::database::sc::new(opt.ns(), opt.db(), &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		run.set(
			key,
			DefineScopeStatement {
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

impl Display for DefineScopeStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE SCOPE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {}", self.name)?;
		if let Some(ref v) = self.session {
			write!(f, " SESSION {v}")?
		}
		if let Some(ref v) = self.signup {
			write!(f, " SIGNUP {v}")?
		}
		if let Some(ref v) = self.signin {
			write!(f, " SIGNIN {v}")?
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}
