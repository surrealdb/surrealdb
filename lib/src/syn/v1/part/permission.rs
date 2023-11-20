use super::super::{
	comment::shouldbespace,
	common::{commas, commasorspace},
	error::expected,
	value::value,
	IResult,
};
use crate::sql::{
	permission::{Permission, PermissionKind},
	Permissions,
};
use nom::{
	branch::alt,
	bytes::complete::tag_no_case,
	combinator::{self, cut, map},
	multi::{separated_list0, separated_list1},
	sequence::tuple,
};

pub fn permissions(i: &str) -> IResult<&str, Permissions> {
	let (i, _) = tag_no_case("PERMISSIONS")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(alt((none, full, specific)))(i)
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
	let (i, perms) = separated_list1(commasorspace, rule)(i)?;
	Ok((
		i,
		Permissions {
			select: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						(PermissionKind::Select, ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or_default(),
			create: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						(PermissionKind::Create, ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or_default(),
			update: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						(PermissionKind::Update, ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or_default(),
			delete: perms
				.iter()
				.find_map(|x| {
					x.iter().find_map(|y| match y {
						(PermissionKind::Delete, ref v) => Some(v.to_owned()),
						_ => None,
					})
				})
				.unwrap_or_default(),
		},
	))
}

pub fn permission(i: &str) -> IResult<&str, Permission> {
	expected(
		"a permission",
		alt((
			combinator::value(Permission::None, tag_no_case("NONE")),
			combinator::value(Permission::Full, tag_no_case("FULL")),
			map(tuple((tag_no_case("WHERE"), shouldbespace, value)), |(_, _, v)| {
				Permission::Specific(v)
			}),
		)),
	)(i)
}

fn rule(i: &str) -> IResult<&str, Vec<(PermissionKind, Permission)>> {
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, kind) = separated_list0(
			commas,
			alt((
				combinator::value(PermissionKind::Select, tag_no_case("SELECT")),
				combinator::value(PermissionKind::Create, tag_no_case("CREATE")),
				combinator::value(PermissionKind::Update, tag_no_case("UPDATE")),
				combinator::value(PermissionKind::Delete, tag_no_case("DELETE")),
			)),
		)(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, expr) = alt((
			combinator::value(Permission::None, tag_no_case("NONE")),
			combinator::value(Permission::Full, tag_no_case("FULL")),
			map(tuple((tag_no_case("WHERE"), shouldbespace, value)), |(_, _, v)| {
				Permission::Specific(v)
			}),
		))(i)?;
		Ok((i, kind.into_iter().map(|k| (k, expr.clone())).collect()))
	})(i)
}

#[cfg(test)]
mod test {
	use crate::sql::{Expression, Value};
	use crate::syn::test::Parse;

	use super::*;

	#[test]
	fn permissions_none() {
		let sql = "PERMISSIONS NONE";
		let res = permissions(sql);
		let out = res.unwrap().1;
		assert_eq!("PERMISSIONS NONE", format!("{}", out));
		assert_eq!(out, Permissions::none());
	}

	#[test]
	fn permissions_full() {
		let sql = "PERMISSIONS FULL";
		let res = permissions(sql);
		let out = res.unwrap().1;
		assert_eq!("PERMISSIONS FULL", format!("{}", out));
		assert_eq!(out, Permissions::full());
	}

	#[test]
	fn permissions_specific() {
		let sql =
			"PERMISSIONS FOR select FULL, FOR create, update WHERE public = true, FOR delete NONE";
		let res = permissions(sql);
		let out = res.unwrap().1;
		assert_eq!(
			"PERMISSIONS FOR select FULL, FOR create, update WHERE public = true, FOR delete NONE",
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

	#[test]
	fn no_empty_permissions() {
		// This was previouslly allowed,
		let sql = "PERMISSION ";
		permission(sql).unwrap_err();
	}
}
