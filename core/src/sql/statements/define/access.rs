use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{AccessDuration, AccessType, Base, Ident, Object, Strand, Value};
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
				let mut run = ctx.tx_lock().await;
				// Clear the cache
				run.clear_cache();
				// Check if access method already exists
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
		write!(f, "DEFINE ACCESS",)?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.base)?;
		match &self.kind {
			AccessType::Jwt(ac) => {
				write!(f, " TYPE JWT {}", ac)?;
			}
			AccessType::Record(ac) => {
				write!(f, " TYPE RECORD")?;
				if let Some(ref v) = ac.signup {
					write!(f, " SIGNUP {v}")?
				}
				if let Some(ref v) = ac.signin {
					write!(f, " SIGNIN {v}")?
				}
				write!(f, " WITH JWT {}", ac.jwt)?;
			}
		}
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

		let mut dur = Object::default();
		dur.insert("grant".to_string(), self.duration.grant.into());
		dur.insert("token".to_string(), self.duration.token.into());
		dur.insert("session".to_string(), self.duration.session.into());
		acc.insert("duration".to_string(), dur.to_string().into());

		if let Some(comment) = comment {
			acc.insert("comment".to_string(), comment.into());
		}

		Value::Object(acc)
	}
}
