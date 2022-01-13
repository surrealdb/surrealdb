use crate::sql::comment::shouldbespace;
use crate::sql::common::commas;
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::{multi::separated_list0, sequence::tuple, IResult};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Permissions {
	pub select: Permission,
	pub create: Permission,
	pub update: Permission,
	pub delete: Permission,
}

impl Permissions {
	fn none() -> Self {
		Permissions {
			select: Permission::None,
			create: Permission::None,
			update: Permission::None,
			delete: Permission::None,
		}
	}

	fn full() -> Self {
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
	let (i, perms) = separated_list0(commas, permission)(i)?;
	Ok((
		i,
		Permissions {
			select: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						(Permission::Select, ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or(Default::default()),
			create: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						(Permission::Create, ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or(Default::default()),
			update: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						(Permission::Update, ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or(Default::default()),
			delete: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						(Permission::Delete, ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or(Default::default()),
		},
	))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Permission {
	None,
	Full,
	Select,
	Create,
	Update,
	Delete,
	Specific(Value),
}

impl Default for Permission {
	fn default() -> Self {
		Permission::None
	}
}

impl fmt::Display for Permission {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Permission::None => write!(f, "NONE"),
			Permission::Full => write!(f, "FULL"),
			Permission::Specific(ref v) => write!(f, "WHERE {}", v),
			_ => write!(f, ""),
		}
	}
}

fn permission(i: &str) -> IResult<&str, Vec<(Permission, Permission)>> {
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, kind) = separated_list0(
		commas,
		alt((
			map(tag_no_case("SELECT"), |_| Permission::Select),
			map(tag_no_case("CREATE"), |_| Permission::Create),
			map(tag_no_case("UPDATE"), |_| Permission::Update),
			map(tag_no_case("DELETE"), |_| Permission::Delete),
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
	Ok((i, kind.iter().map(|k| (k.to_owned(), expr.clone())).collect()))
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
