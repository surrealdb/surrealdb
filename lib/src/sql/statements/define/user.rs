use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::iam::Role;
use crate::sql::base::{base, Base};
use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::ending;
use crate::sql::error::expect_tag_no_case;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::error::ParseError as SqlError;
use crate::sql::escape::quote_str;
use crate::sql::fmt::Fmt;
use crate::sql::ident::{ident, Ident};
use crate::sql::strand::{strand, strand_raw, Strand};
use crate::sql::value::Value;
use argon2::password_hash::{PasswordHasher, SaltString};
use argon2::Argon2;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use nom::multi::many0;
use nom::multi::separated_list1;
use nom::Err::Failure;
use rand::distributions::Alphanumeric;
use rand::rngs::OsRng;
use rand::Rng;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineUserStatement {
	pub name: Ident,
	pub base: Base,
	pub hash: String,
	pub code: String,
	pub roles: Vec<Ident>,
	pub comment: Option<Strand>,
}

impl From<(Base, &str, &str)> for DefineUserStatement {
	fn from((base, user, pass): (Base, &str, &str)) -> Self {
		DefineUserStatement {
			base,
			name: user.into(),
			hash: Argon2::default()
				.hash_password(pass.as_ref(), &SaltString::generate(&mut OsRng))
				.unwrap()
				.to_string(),
			code: rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(128)
				.map(char::from)
				.collect::<String>(),
			roles: vec!["owner".into()],
			comment: None,
		}
	}
}

impl DefineUserStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match self.base {
			Base::Root => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::root::us::new(&self.name);
				run.set(key, self).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::namespace::us::new(opt.ns(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.set(key, self).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::database::us::new(opt.ns(), opt.db(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.add_db(opt.ns(), opt.db(), opt.strict).await?;
				run.set(key, self).await?;
				// Ok all good
				Ok(Value::None)
			}
			// Other levels are not supported
			_ => Err(Error::InvalidLevel(self.base.to_string())),
		}
	}
}

impl Display for DefineUserStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"DEFINE USER {} ON {} PASSHASH {} ROLES {}",
			self.name,
			self.base,
			quote_str(&self.hash),
			Fmt::comma_separated(
				&self.roles.iter().map(|r| r.to_string().to_uppercase()).collect::<Vec<String>>()
			)
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

pub fn user(i: &str) -> IResult<&str, DefineUserStatement> {
	let (i, _) = tag_no_case("USER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, (name, base, opts)) = cut(|i| {
		let (i, name) = ident(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("ON")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, base) = base(i)?;
		let (i, opts) = user_opts(i)?;
		let (i, _) = expected("PASSWORD, PASSHASH, ROLES, or COMMENT", ending::query)(i)?;
		Ok((i, (name, base, opts)))
	})(i)?;
	// Create the base statement
	let mut res = DefineUserStatement {
		name,
		base,
		roles: vec!["Viewer".into()], // New users get the viewer role by default
		code: rand::thread_rng()
			.sample_iter(&Alphanumeric)
			.take(128)
			.map(char::from)
			.collect::<String>(),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineUserOption::Password(v) => {
				res.hash = Argon2::default()
					.hash_password(v.as_ref(), &SaltString::generate(&mut OsRng))
					.unwrap()
					.to_string()
			}
			DefineUserOption::Passhash(v) => {
				res.hash = v;
			}
			DefineUserOption::Roles(v) => {
				res.roles = v;
			}
			DefineUserOption::Comment(v) => {
				res.comment = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineUserOption {
	Password(String),
	Passhash(String),
	Roles(Vec<Ident>),
	Comment(Strand),
}

fn user_opts(i: &str) -> IResult<&str, Vec<DefineUserOption>> {
	many0(alt((user_pass, user_hash, user_roles, user_comment)))(i)
}

fn user_pass(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PASSWORD")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand_raw)(i)?;
	Ok((i, DefineUserOption::Password(v)))
}

fn user_hash(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("PASSHASH")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand_raw)(i)?;
	Ok((i, DefineUserOption::Passhash(v)))
}

fn user_comment(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineUserOption::Comment(v)))
}

fn user_roles(i: &str) -> IResult<&str, DefineUserOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ROLES")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, roles) = separated_list1(commas, |i| {
		let (i, v) = cut(ident)(i)?;
		// Verify the role is valid
		Role::try_from(v.as_str()).map_err(|_| Failure(SqlError::Role(i, v.to_string())))?;

		Ok((i, v))
	})(i)?;

	Ok((i, DefineUserOption::Roles(roles)))
}
