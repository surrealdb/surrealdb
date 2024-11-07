use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Actor, Auth, Level, Role};
use crate::sql::fmt::Fmt;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Ident, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum RunAsKind {
	User(Ident),
	Roles(Vec<Ident>),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RunAs {
	pub base: Base,
	pub kind: RunAsKind,
}

impl fmt::Display for RunAsKind {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			RunAsKind::User(user) => {
				write!(f, "USER {user}")?;
			}
			RunAsKind::Roles(roles) => {
				write!(
					f,
					"ROLES {}",
					Fmt::comma_separated(
						&roles
							.iter()
							.map(|r| r.to_string().to_uppercase())
							.collect::<Vec<String>>()
					),
				)?;
			}
		}
		Ok(())
	}
}

impl InfoStructure for RunAsKind {
	fn structure(self) -> Value {
		match self {
			RunAsKind::User(user) => Value::from(map! {
				"USER".to_string() => user.structure(),
			}),
			RunAsKind::Roles(roles) => Value::from(map! {
				"ROLES".to_string() => roles.into_iter().map(Ident::structure).collect::<_>(),
			}),
		}
	}
}

impl fmt::Display for RunAs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "AS {} {}", self.base, self.kind)
	}
}

impl InfoStructure for RunAs {
	fn structure(self) -> Value {
		Value::from(map! {
		  "base".to_string() => self.base.structure(),
		  "kind".to_string() => self.kind.structure(),
		})
	}
}

impl RunAs {
	/// Create a new RunAs with a user
	pub fn as_user(user: impl Into<Ident>) -> Self {
		Self {
			base: Base::Db,
			kind: RunAsKind::User(user.into()),
		}
	}

	/// Create a new RunAs with roles
	pub fn as_roles<I>(roles: I) -> Self
	where
		I: IntoIterator,
		I::Item: Into<Ident>,
	{
		Self {
			base: Base::Db,
			kind: RunAsKind::Roles(roles.into_iter().map(Into::into).collect()),
		}
	}

	/// Chainable method to set the base level.
	pub fn with_base(mut self, base: Base) -> Self {
		self.base = base;
		self
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn to_auth(&self, ctx: &Context, opt: &Options) -> Result<Auth, Error> {
		let level = match self.base {
			Base::Root => Level::Root,
			Base::Ns => (opt.ns()?,).into(),
			Base::Db => (opt.ns()?, opt.db()?).into(),
			_ => return Err(Error::InvalidLevel(self.base.to_string())),
		};

		match &self.kind {
			RunAsKind::User(name) => {
				let txn = ctx.tx();
				let user = match self.base {
					Base::Root => txn.get_root_user(name).await,
					Base::Ns => txn.get_ns_user(opt.ns()?, name).await,
					Base::Db => txn.get_db_user(opt.ns()?, opt.db()?, name).await,
					_ => Err(Error::InvalidLevel(self.base.to_string())),
				};
				Ok((user?.as_ref(), level).into())
			}
			RunAsKind::Roles(roles) => {
				let name = "system_auth".into();
				let roles: Result<Vec<Role>, _> =
					roles.iter().map(|r| Role::from_str(r.as_str())).collect();
				Ok(Auth::new(Actor::new(name, roles?, level)))
			}
		}
	}
}
