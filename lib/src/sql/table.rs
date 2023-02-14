use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::escape::escape_ident;
use crate::sql::fmt::Fmt;
use crate::sql::id::Id;
use crate::sql::ident::{ident_raw, Ident};
use crate::sql::serde::is_internal_serialization;
use crate::sql::thing::Thing;
use nom::multi::separated_list1;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Table";

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Tables(pub Vec<Table>);

impl From<Table> for Tables {
	fn from(v: Table) -> Self {
		Tables(vec![v])
	}
}

impl Deref for Tables {
	type Target = Vec<Table>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Tables {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

pub fn tables(i: &str) -> IResult<&str, Tables> {
	let (i, v) = separated_list1(commas, table)(i)?;
	Ok((i, Tables(v)))
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Table(pub String);

impl From<String> for Table {
	fn from(v: String) -> Self {
		Self(v)
	}
}

impl From<&str> for Table {
	fn from(v: &str) -> Self {
		Self::from(String::from(v))
	}
}

impl From<Ident> for Table {
	fn from(v: Ident) -> Self {
		Self(v.0)
	}
}

impl Deref for Table {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Table {
	pub fn generate(&self) -> Thing {
		Thing {
			tb: self.0.to_owned(),
			id: Id::rand(),
		}
	}
}

impl Display for Table {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&escape_ident(&self.0), f)
	}
}

impl Serialize for Table {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct(TOKEN, &self.0)
		} else {
			serializer.serialize_none()
		}
	}
}

pub fn table(i: &str) -> IResult<&str, Table> {
	let (i, v) = ident_raw(i)?;
	Ok((i, Table(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn table_normal() {
		let sql = "test";
		let res = table(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Table(String::from("test")));
	}

	#[test]
	fn table_quoted_backtick() {
		let sql = "`test`";
		let res = table(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Table(String::from("test")));
	}

	#[test]
	fn table_quoted_brackets() {
		let sql = "⟨test⟩";
		let res = table(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Table(String::from("test")));
	}
}
