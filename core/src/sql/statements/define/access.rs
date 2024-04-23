use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::access_type::JwtAccessVerification;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{escape::quote_str, AccessType, Base, Ident, Object, Strand, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineAccessStatement {
	pub name: Ident,
	pub base: Base,
	pub kind: AccessType,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineAccessStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match &self.base {
			// TODO(PR): Support root level definition.
			Base::Ns => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Check if token already exists
				if self.if_not_exists && run.get_ns_access(opt.ns(), &self.name).await.is_ok() {
					return Err(Error::AccessNsAlreadyExists {
						value: self.name.to_string(),
					});
				}
				// Process the statement
				let key = crate::key::namespace::ac::new(opt.ns(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.set(
					key,
					DefineAccessStatement {
						if_not_exists: false,
						..self.clone()
					},
				)
				.await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Check if token already exists
				if self.if_not_exists
					&& run.get_db_access(opt.ns(), opt.db(), &self.name).await.is_ok()
				{
					return Err(Error::AccessDbAlreadyExists {
						value: self.name.to_string(),
					});
				}
				// Process the statement
				let key = crate::key::database::ac::new(opt.ns(), opt.db(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.add_db(opt.ns(), opt.db(), opt.strict).await?;
				run.set(
					key,
					DefineAccessStatement {
						if_not_exists: false,
						..self.clone()
					},
				)
				.await?;
				// Ok all good
				Ok(Value::None)
			}
			// Other levels are not supported
			_ => Err(Error::InvalidLevel(self.base.to_string())),
		}
	}
}

impl Display for DefineAccessStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TOKEN",)?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.base)?;
		match &self.kind {
			AccessType::Jwt(ac) => {
				write!(f, " TYPE JWT")?;
				match &ac.verification {
					JwtAccessVerification::Key(ref v) => {
						write!(f, " ALGORITHM {} KEY {}", v.alg, quote_str(&v.key))?;
					}
					JwtAccessVerification::Jwks(ref v) => {
						write!(f, " JWKS {}", quote_str(&v.url),)?;
					}
				}
			}
			AccessType::Record(ac) => {
				write!(f, " TYPE RECORD")?;
				if let Some(ref v) = ac.duration {
					write!(f, " DURATION {v}")?
				}
				if let Some(ref v) = ac.signup {
					write!(f, " SIGNUP {v}")?
				}
				if let Some(ref v) = ac.signin {
					write!(f, " SIGNIN {v}")?
				}
			}
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineAccessStatement {
	fn structure(self) -> Value {
		let Self {
			name,
			base,
			kind,
			comment,
			..
		} = self;
		let mut acc = Object::default();

		acc.insert("name".to_string(), name.structure());

		acc.insert("base".to_string(), base.structure());

		acc.insert("kind".to_string(), kind.structure());

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		Value::Object(acc)
	}
}
