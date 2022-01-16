use crate::dbs::Auth;
use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::ident_raw;
use crate::sql::value::Value;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UseStatement {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ns: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub db: Option<String>,
}

impl UseStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		opt: &Options<'_>,
		exe: &mut Executor,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		if let Some(ns) = &self.ns {
			match opt.auth {
				Auth::No => exe.ns = Some(ns.to_owned()),
				Auth::Kv => exe.ns = Some(ns.to_owned()),
				Auth::Ns(v) if v == ns => exe.ns = Some(ns.to_owned()),
				_ => {
					exe.ns = None;
					return Err(Error::NsAuthenticationError {
						ns: ns.to_owned(),
					});
				}
			}
		}
		if let Some(db) = &self.db {
			match opt.auth {
				Auth::No => exe.db = Some(db.to_owned()),
				Auth::Kv => exe.db = Some(db.to_owned()),
				Auth::Ns(_) => exe.db = Some(db.to_owned()),
				Auth::Db(_, v) if v == db => exe.db = Some(db.to_owned()),
				_ => {
					exe.db = None;
					return Err(Error::DbAuthenticationError {
						db: db.to_owned(),
					});
				}
			}
		}
		Ok(Value::None)
	}
}

impl fmt::Display for UseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "USE")?;
		if let Some(ref ns) = self.ns {
			write!(f, " NS {}", ns)?;
		}
		if let Some(ref db) = self.db {
			write!(f, " DB {}", db)?;
		}
		Ok(())
	}
}

pub fn yuse(i: &str) -> IResult<&str, UseStatement> {
	alt((both, ns, db))(i)
}

fn both(i: &str) -> IResult<&str, UseStatement> {
	let (i, _) = tag_no_case("USE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("NAMESPACE"), tag_no_case("NS")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, ns) = ident_raw(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("DATABASE"), tag_no_case("DB")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, db) = ident_raw(i)?;
	Ok((
		i,
		UseStatement {
			ns: Some(ns),
			db: Some(db),
		},
	))
}

fn ns(i: &str) -> IResult<&str, UseStatement> {
	let (i, _) = tag_no_case("USE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("NAMESPACE"), tag_no_case("NS")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, ns) = ident_raw(i)?;
	Ok((
		i,
		UseStatement {
			ns: Some(ns),
			db: None,
		},
	))
}

fn db(i: &str) -> IResult<&str, UseStatement> {
	let (i, _) = tag_no_case("USE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("DATABASE"), tag_no_case("DB")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, db) = ident_raw(i)?;
	Ok((
		i,
		UseStatement {
			ns: None,
			db: Some(db),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn use_query_ns() {
		let sql = "USE NS test";
		let res = yuse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			UseStatement {
				ns: Some(String::from("test")),
				db: None,
			}
		);
		assert_eq!("USE NS test", format!("{}", out));
	}

	#[test]
	fn use_query_db() {
		let sql = "USE DB test";
		let res = yuse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			UseStatement {
				ns: None,
				db: Some(String::from("test")),
			}
		);
		assert_eq!("USE DB test", format!("{}", out));
	}

	#[test]
	fn use_query_both() {
		let sql = "USE NS test DB test";
		let res = yuse(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			out,
			UseStatement {
				ns: Some(String::from("test")),
				db: Some(String::from("test")),
			}
		);
		assert_eq!("USE NS test DB test", format!("{}", out));
	}
}
