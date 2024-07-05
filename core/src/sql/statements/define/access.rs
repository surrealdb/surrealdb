use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{access::AccessDuration, AccessType, Base, Ident, Strand, Value};
use derive::Store;
use rand::distributions::Alphanumeric;
use rand::Rng;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineAccessStatement {
	pub name: Ident,
	pub base: Base,
	pub kind: AccessType,
	pub duration: AccessDuration,
	pub comment: Option<Strand>,
	pub if_not_exists: bool,
}

impl DefineAccessStatement {
	/// Generate a random key to be used to sign session tokens
	/// This key will be used to sign tokens issued with this access method
	/// This value is used by default in every access method other than JWT
	pub(crate) fn random_key() -> String {
		rand::thread_rng().sample_iter(&Alphanumeric).take(128).map(char::from).collect::<String>()
	}

	/// Returns a version of the statement where potential secrets are redacted
	/// This function should be used when displaying the statement to datastore users
	/// This function should NOT be used when displaying the statement for export purposes
	pub fn redacted(&self) -> DefineAccessStatement {
		let mut das = self.clone();
		das.kind = match das.kind {
			AccessType::Jwt(ac) => AccessType::Jwt(ac.redacted()),
			AccessType::Record(mut ac) => {
				ac.jwt = ac.jwt.redacted();
				AccessType::Record(ac)
			}
			AccessType::Bearer(mut ac) => {
				ac.jwt = ac.jwt.redacted();
				AccessType::Bearer(ac)
			}
		};
		das
	}
}

impl DefineAccessStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match &self.base {
			Base::Ns => {
				// Claim transaction
				let mut run = ctx.tx_lock().await;
				// Clear the cache
				run.clear_cache();
				// Check if access method already exists
				if run.get_ns_access(opt.ns()?, &self.name).await.is_ok() {
					if self.if_not_exists {
						return Ok(Value::None);
					} else {
						return Err(Error::AccessNsAlreadyExists {
							value: self.name.to_string(),
						});
					}
				}
				// Process the statement
				let key = crate::key::namespace::access::ac::new(opt.ns()?, &self.name);
				run.add_ns(opt.ns()?, opt.strict).await?;
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
				let mut run = ctx.tx_lock().await;
				// Clear the cache
				run.clear_cache();
				// Check if access method already exists
				if run.get_db_access(opt.ns()?, opt.db()?, &self.name).await.is_ok() {
					if self.if_not_exists {
						return Ok(Value::None);
					} else {
						return Err(Error::AccessDbAlreadyExists {
							value: self.name.to_string(),
						});
					}
				}
				// Process the statement
				let key = crate::key::database::access::ac::new(opt.ns()?, opt.db()?, &self.name);
				run.add_ns(opt.ns()?, opt.strict).await?;
				run.add_db(opt.ns()?, opt.db()?, opt.strict).await?;
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
		write!(f, "DEFINE ACCESS",)?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		// The specific access method definition is displayed by AccessType
		write!(f, " {} ON {} TYPE {}", self.name, self.base, self.kind)?;
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		if self.kind.can_issue_grants() {
			write!(
				f,
				" FOR GRANT {},",
				match self.duration.grant {
					Some(dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		if self.kind.can_issue_tokens() {
			write!(
				f,
				" FOR TOKEN {},",
				match self.duration.token {
					Some(dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		write!(
			f,
			" FOR SESSION {}",
			match self.duration.session {
				Some(dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineAccessStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"base".to_string() => self.base.structure(),
			"duration".to_string() => Value::from(map!{
				"session".to_string() => self.duration.session.into(),
				"grant".to_string(), if self.kind.can_issue_grants() => self.duration.grant.into(),
				"token".to_string(), if self.kind.can_issue_tokens() => self.duration.token.into(),
			}),
			"kind".to_string() => self.kind.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
