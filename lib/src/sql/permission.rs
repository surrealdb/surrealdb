use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::common::commasorspace;
use crate::sql::error::IResult;
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::{multi::separated_list0, sequence::tuple};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Permissions {
	pub select: Permission,
	pub create: Permission,
	pub update: Permission,
	pub delete: Permission,
}

impl Permissions {
	pub fn none() -> Self {
		Permissions {
			select: Permission::None,
			create: Permission::None,
			update: Permission::None,
			delete: Permission::None,
		}
	}

	pub fn full() -> Self {
		Permissions {
			select: Permission::Full,
			create: Permission::Full,
			update: Permission::Full,
			delete: Permission::Full,
		}
	}

	pub fn is_none(&self) -> bool {
		self.select == Permission::None
			&& self.create == Permission::None
			&& self.update == Permission::None
			&& self.delete == Permission::None
	}

	pub fn is_full(&self) -> bool {
		self.select == Permission::Full
			&& self.create == Permission::Full
			&& self.update == Permission::Full
			&& self.delete == Permission::Full
	}
}

impl fmt::Display for Permissions {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "PERMISSIONS")?;
		if self.is_none() {
			return write!(f, " NONE");
		}
		if self.is_full() {
			return write!(f, " FULL");
		}
		write!(
			f,
			" FOR select {}, FOR create {}, FOR update {}, FOR delete {}",
			self.select, self.create, self.update, self.delete
		)
	}
}

pub fn permissions(i: &str) -> IResult<&str, Permissions> {
	let (i, _) = tag_no_case("PERMISSIONS")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((none, full, specific))(i)
}

fn none(i: &str) -> IResult<&str, Permissions> {
	let (i, _) = tag_no_case("NONE")(i)?;
	Ok((i, Permissions::none()))
}

fn full(i: &str) -> IResult<&str, Permissions> {
	let (i, _) = tag_no_case("FULL")(i)?;
	Ok((i, Permissions::full()))
}

fn specific(i: &str) -> IResult<&str, Permissions> {
	let (i, perms) = separated_list0(commasorspace, permission)(i)?;
	Ok((
		i,
		Permissions {
			select: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						('s', ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or_default(),
			create: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						('c', ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or_default(),
			update: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						('u', ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or_default(),
			delete: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						('d', ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or_default(),
		},
	))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Permission {
	None,
	Full,
	Specific(Value),
}

impl Default for Permission {
	fn default() -> Self {
		Self::Full
	}
}

impl fmt::Display for Permission {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::None => f.write_str("NONE"),
			Self::Full => f.write_str("FULL"),
			Self::Specific(ref v) => write!(f, "WHERE {}", v),
		}
	}
}

fn permission(i: &str) -> IResult<&str, Vec<(char, Permission)>> {
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, kind) = separated_list0(
		commas,
		alt((
			map(tag_no_case("SELECT"), |_| 's'),
			map(tag_no_case("CREATE"), |_| 'c'),
			map(tag_no_case("UPDATE"), |_| 'u'),
			map(tag_no_case("DELETE"), |_| 'd'),
		)),
	)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, expr) = alt((
		map(tag_no_case("NONE"), |_| Permission::None),
		map(tag_no_case("FULL"), |_| Permission::Full),
		map(tuple((tag_no_case("WHERE"), shouldbespace, value)), |(_, _, v)| {
			Permission::Specific(v)
		}),
	))(i)?;
	Ok((i, kind.into_iter().map(|k| (k, expr.clone())).collect()))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::expression::Expression;
	use crate::sql::test::Parse;

	#[test]
	fn permissions_none() {
		let sql = "PERMISSIONS NONE";
		let res = permissions(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("PERMISSIONS NONE", format!("{}", out));
		assert_eq!(out, Permissions::none());
	}

	#[test]
	fn permissions_full() {
		let sql = "PERMISSIONS FULL";
		let res = permissions(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("PERMISSIONS FULL", format!("{}", out));
		assert_eq!(out, Permissions::full());
	}

	#[test]
	fn permissions_specific() {
		let sql =
			"PERMISSIONS FOR select FULL, FOR create, update WHERE public = true, FOR delete NONE";
		let res = permissions(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			"PERMISSIONS FOR select FULL, FOR create WHERE public = true, FOR update WHERE public = true, FOR delete NONE",
			format!("{}", out)
		);
		assert_eq!(
			out,
			Permissions {
				select: Permission::Full,
				create: Permission::Specific(Value::from(Expression::parse("public = true"))),
				update: Permission::Specific(Value::from(Expression::parse("public = true"))),
				delete: Permission::None,
			}
		);
	}
}
