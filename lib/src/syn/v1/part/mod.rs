use super::{
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	error::{expect_tag_no_case, expected},
	idiom::{basic, plain},
	literal::{datetime, duration, ident, scoring, table, tables},
	operator::{assigner, dir},
	thing::thing,
	// TODO: go through and check every import for alias.
	value::value,
	IResult,
};
use crate::sql::{
	Base, ChangeFeed, Cond, Data, Edges, Explain, Fetch, Fetchs, Group, Groups, Limit, Order,
	Orders, Output, Tables, Version,
};
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, into, map, map_res, opt, recognize, value as map_value},
	multi::separated_list1,
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

pub mod data;
pub mod field;
pub mod index;
pub mod permission;
pub mod split;
pub mod start;
pub mod view;
pub mod with;

pub use data::data;
pub use field::{field, fields};
pub use split::split;
pub use start::start;
pub use view::view;
pub use with::with;

pub fn base(i: &str) -> IResult<&str, Base> {
	expected(
		"a base, one of NAMESPACE, DATABASE, ROOT or KV",
		alt((
			map_value(Base::Ns, tag_no_case("NAMESPACE")),
			map_value(Base::Db, tag_no_case("DATABASE")),
			map_value(Base::Root, tag_no_case("ROOT")),
			map_value(Base::Ns, tag_no_case("NS")),
			map_value(Base::Db, tag_no_case("DB")),
			map_value(Base::Root, tag_no_case("KV")),
		)),
	)(i)
}

pub fn base_or_scope(i: &str) -> IResult<&str, Base> {
	alt((base, |i| {
		let (i, _) = tag_no_case("SCOPE")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, v) = cut(ident)(i)?;
		Ok((i, Base::Sc(v)))
	}))(i)
}

pub fn changefeed(i: &str) -> IResult<&str, ChangeFeed> {
	let (i, _) = tag_no_case("CHANGEFEED")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(duration)(i)?;
	Ok((
		i,
		ChangeFeed {
			expiry: v.0,
		},
	))
}

pub fn cond(i: &str) -> IResult<&str, Cond> {
	let (i, _) = tag_no_case("WHERE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(value)(i)?;
	Ok((i, Cond(v)))
}

pub fn edges(i: &str) -> IResult<&str, Edges> {
	let (i, from) = thing(i)?;
	let (i, dir) = dir(i)?;
	let (i, what) = alt((simple, custom))(i)?;
	Ok((
		i,
		Edges {
			dir,
			from,
			what,
		},
	))
}

fn simple(i: &str) -> IResult<&str, Tables> {
	alt((any, one))(i)
}

fn custom(i: &str) -> IResult<&str, Tables> {
	let (i, _) = openparentheses(i)?;
	let (i, w) = alt((any, tables))(i)?;
	let (i, _) = cut(closeparentheses)(i)?;
	Ok((i, w))
}

fn one(i: &str) -> IResult<&str, Tables> {
	into(table)(i)
}

fn any(i: &str) -> IResult<&str, Tables> {
	map(char('?'), |_| Tables::default())(i)
}

pub fn explain(i: &str) -> IResult<&str, Explain> {
	let (i, _) = tag_no_case("EXPLAIN")(i)?;
	let (i, full) = opt(tuple((shouldbespace, tag_no_case("FULL"))))(i)?;
	Ok((i, Explain(full.is_some())))
}

pub fn fetch(i: &str) -> IResult<&str, Fetchs> {
	let (i, _) = tag_no_case("FETCH")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(separated_list1(commas, fetch_raw))(i)?;
	Ok((i, Fetchs(v)))
}

fn fetch_raw(i: &str) -> IResult<&str, Fetch> {
	let (i, v) = plain(i)?;
	Ok((i, Fetch(v)))
}

pub fn group(i: &str) -> IResult<&str, Groups> {
	let (i, _) = tag_no_case("GROUP")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(alt((group_all, group_any)))(i)
}

fn group_all(i: &str) -> IResult<&str, Groups> {
	let (i, _) = tag_no_case("ALL")(i)?;
	Ok((i, Groups(vec![])))
}

fn group_any(i: &str) -> IResult<&str, Groups> {
	let (i, _) = opt(terminated(tag_no_case("BY"), shouldbespace))(i)?;
	let (i, v) = separated_list1(commas, group_raw)(i)?;
	Ok((i, Groups(v)))
}

fn group_raw(i: &str) -> IResult<&str, Group> {
	let (i, v) = basic(i)?;
	Ok((i, Group(v)))
}

pub fn limit(i: &str) -> IResult<&str, Limit> {
	let (i, _) = tag_no_case("LIMIT")(i)?;
	cut(|i| {
		let (i, _) = opt(tuple((shouldbespace, tag_no_case("BY"))))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, v) = value(i)?;
		Ok((i, Limit(v)))
	})(i)
}

pub fn order(i: &str) -> IResult<&str, Orders> {
	let (i, _) = tag_no_case("ORDER")(i)?;
	cut(|i| {
		let (i, _) = opt(tuple((shouldbespace, tag_no_case("BY"))))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, v) = alt((order_rand, separated_list1(commas, order_raw)))(i)?;
		Ok((i, Orders(v)))
	})(i)
}

fn order_rand(i: &str) -> IResult<&str, Vec<Order>> {
	let (i, _) = tag_no_case("RAND()")(i)?;
	Ok((
		i,
		vec![Order {
			order: Default::default(),
			random: true,
			collate: false,
			numeric: false,
			direction: true,
		}],
	))
}

fn order_raw(i: &str) -> IResult<&str, Order> {
	let (i, v) = basic(i)?;
	let (i, c) = opt(tuple((shouldbespace, tag_no_case("COLLATE"))))(i)?;
	let (i, n) = opt(tuple((shouldbespace, tag_no_case("NUMERIC"))))(i)?;
	let (i, d) = opt(alt((
		value(true, tuple((shouldbespace, tag_no_case("ASC")))),
		value(false, tuple((shouldbespace, tag_no_case("DESC")))),
	)))(i)?;
	Ok((
		i,
		Order {
			order: v,
			random: false,
			collate: c.is_some(),
			numeric: n.is_some(),
			direction: d.unwrap_or(true),
		},
	))
}

pub fn output(i: &str) -> IResult<&str, Output> {
	let (i, _) = tag_no_case("RETURN")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, v) = alt((
			value(Output::None, tag_no_case("NONE")),
			value(Output::Null, tag_no_case("NULL")),
			value(Output::Diff, tag_no_case("DIFF")),
			value(Output::After, tag_no_case("AFTER")),
			value(Output::Before, tag_no_case("BEFORE")),
			map(fields, Output::Fields),
		))(i)?;
		Ok((i, v))
	})(i)
}

pub fn version(i: &str) -> IResult<&str, Version> {
	let (i, _) = tag_no_case("VERSION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(datetime)(i)?;
	Ok((i, Version(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn block_empty() {
		let sql = "{}";
		let res = block(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn block_value() {
		let sql = "{ 80 }";
		let res = block(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn block_ifelse() {
		let sql = "{ RETURN IF true THEN 50 ELSE 40 END; }";
		let res = block(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}

	#[test]
	fn block_multiple() {
		let sql = r#"{

	LET $person = (SELECT * FROM person WHERE first = $first AND last = $last AND birthday = $birthday);

	RETURN IF $person[0].id THEN
		$person[0]
	ELSE
		(CREATE person SET first = $first, last = $last, birthday = $birthday)
	END;

}"#;
		let res = block(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{:#}", out))
	}

	#[test]
	fn changefeed_missing() {
		let sql: &str = "";
		let res = changefeed(sql);
		assert!(res.is_err());
	}

	#[test]
	fn changefeed_enabled() {
		let sql = "CHANGEFEED 1h";
		let res = changefeed(sql);
		let out = res.unwrap().1;
		assert_eq!("CHANGEFEED 1h", format!("{}", out));
		assert_eq!(
			out,
			ChangeFeed {
				expiry: time::Duration::from_secs(3600)
			}
		);
	}

	#[test]
	fn cond_statement() {
		let sql = "WHERE field = true";
		let res = cond(sql);
		let out = res.unwrap().1;
		assert_eq!("WHERE field = true", format!("{}", out));
	}

	#[test]
	fn cond_statement_multiple() {
		let sql = "WHERE field = true AND other.field = false";
		let res = cond(sql);
		let out = res.unwrap().1;
		assert_eq!("WHERE field = true AND other.field = false", format!("{}", out));
	}

	#[test]
	fn data_set_statement() {
		let sql = "SET field = true";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("SET field = true", format!("{}", out));
	}

	#[test]
	fn data_set_statement_multiple() {
		let sql = "SET field = true, other.field = false";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("SET field = true, other.field = false", format!("{}", out));
	}

	#[test]
	fn data_unset_statement() {
		let sql = "UNSET field";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("UNSET field", format!("{}", out));
	}

	#[test]
	fn data_unset_statement_multiple_fields() {
		let sql = "UNSET field, other.field";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("UNSET field, other.field", format!("{}", out));
	}

	#[test]
	fn data_patch_statement() {
		let sql = "PATCH [{ field: true }]";
		let res = patch(sql);
		let out = res.unwrap().1;
		assert_eq!("PATCH [{ field: true }]", format!("{}", out));
	}

	#[test]
	fn data_merge_statement() {
		let sql = "MERGE { field: true }";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("MERGE { field: true }", format!("{}", out));
	}

	#[test]
	fn data_content_statement() {
		let sql = "CONTENT { field: true }";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("CONTENT { field: true }", format!("{}", out));
	}

	#[test]
	fn data_replace_statement() {
		let sql = "REPLACE { field: true }";
		let res = data(sql);
		let out = res.unwrap().1;
		assert_eq!("REPLACE { field: true }", format!("{}", out));
	}

	#[test]
	fn data_values_statement() {
		let sql = "(one, two, three) VALUES ($param, true, [1, 2, 3]), ($param, false, [4, 5, 6])";
		let res = values(sql);
		let out = res.unwrap().1;
		assert_eq!(
			"(one, two, three) VALUES ($param, true, [1, 2, 3]), ($param, false, [4, 5, 6])",
			format!("{}", out)
		);
	}

	#[test]
	fn data_update_statement() {
		let sql = "ON DUPLICATE KEY UPDATE field = true, other.field = false";
		let res = update(sql);
		let out = res.unwrap().1;
		assert_eq!("ON DUPLICATE KEY UPDATE field = true, other.field = false", format!("{}", out));
	}

	#[test]
	fn dir_in() {
		let sql = "<-";
		let res = dir(sql);
		let out = res.unwrap().1;
		assert_eq!("<-", format!("{}", out));
	}

	#[test]
	fn dir_out() {
		let sql = "->";
		let res = dir(sql);
		let out = res.unwrap().1;
		assert_eq!("->", format!("{}", out));
	}

	#[test]
	fn dir_both() {
		let sql = "<->";
		let res = dir(sql);
		let out = res.unwrap().1;
		assert_eq!("<->", format!("{}", out));
	}

	#[test]
	fn edges_in() {
		let sql = "person:test<-likes";
		let res = edges(sql);
		let out = res.unwrap().1;
		assert_eq!("person:test<-likes", format!("{}", out));
	}

	#[test]
	fn edges_out() {
		let sql = "person:test->likes";
		let res = edges(sql);
		let out = res.unwrap().1;
		assert_eq!("person:test->likes", format!("{}", out));
	}

	#[test]
	fn edges_both() {
		let sql = "person:test<->likes";
		let res = edges(sql);
		let out = res.unwrap().1;
		assert_eq!("person:test<->likes", format!("{}", out));
	}

	#[test]
	fn edges_multiple() {
		let sql = "person:test->(likes, follows)";
		let res = edges(sql);
		let out = res.unwrap().1;
		assert_eq!("person:test->(likes, follows)", format!("{}", out));
	}

	#[test]
	fn explain_statement() {
		let sql = "EXPLAIN";
		let res = explain(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Explain(false));
		assert_eq!("EXPLAIN", format!("{}", out));
	}

	#[test]
	fn explain_full_statement() {
		let sql = "EXPLAIN FULL";
		let res = explain(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Explain(true));
		assert_eq!("EXPLAIN FULL", format!("{}", out));
	}

	#[test]
	fn fetch_statement() {
		let sql = "FETCH field";
		let res = fetch(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Fetchs(vec![Fetch(Idiom::parse("field"))]));
		assert_eq!("FETCH field", format!("{}", out));
	}

	#[test]
	fn fetch_statement_multiple() {
		let sql = "FETCH field, other.field";
		let res = fetch(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Fetchs(vec![Fetch(Idiom::parse("field")), Fetch(Idiom::parse("other.field")),])
		);
		assert_eq!("FETCH field, other.field", format!("{}", out));
	}

	#[test]
	fn field_all() {
		let sql = "*";
		let res = fields(sql);
		let out = res.unwrap().1;
		assert_eq!("*", format!("{}", out));
	}

	#[test]
	fn field_one() {
		let sql = "field";
		let res = fields(sql);
		let out = res.unwrap().1;
		assert_eq!("field", format!("{}", out));
	}

	#[test]
	fn field_value() {
		let sql = "VALUE field";
		let res = fields(sql);
		let out = res.unwrap().1;
		assert_eq!("VALUE field", format!("{}", out));
	}

	#[test]
	fn field_alias() {
		let sql = "field AS one";
		let res = fields(sql);
		let out = res.unwrap().1;
		assert_eq!("field AS one", format!("{}", out));
	}

	#[test]
	fn field_value_alias() {
		let sql = "VALUE field AS one";
		let res = fields(sql);
		let out = res.unwrap().1;
		assert_eq!("VALUE field AS one", format!("{}", out));
	}

	#[test]
	fn field_multiple() {
		let sql = "field, other.field";
		let res = fields(sql);
		let out = res.unwrap().1;
		assert_eq!("field, other.field", format!("{}", out));
	}

	#[test]
	fn field_aliases() {
		let sql = "field AS one, other.field AS two";
		let res = fields(sql);
		let out = res.unwrap().1;
		assert_eq!("field AS one, other.field AS two", format!("{}", out));
	}

	#[test]
	fn field_value_only_one() {
		let sql = "VALUE field, other.field";
		fields(sql).unwrap_err();
	}

	#[test]
	fn group_statement() {
		let sql = "GROUP field";
		let res = group(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Groups(vec![Group(Idiom::parse("field"))]));
		assert_eq!("GROUP BY field", format!("{}", out));
	}

	#[test]
	fn group_statement_by() {
		let sql = "GROUP BY field";
		let res = group(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Groups(vec![Group(Idiom::parse("field"))]));
		assert_eq!("GROUP BY field", format!("{}", out));
	}

	#[test]
	fn group_statement_multiple() {
		let sql = "GROUP field, other.field";
		let res = group(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Groups(vec![Group(Idiom::parse("field")), Group(Idiom::parse("other.field"))])
		);
		assert_eq!("GROUP BY field, other.field", format!("{}", out));
	}

	#[test]
	fn group_statement_all() {
		let sql = "GROUP ALL";
		let out = group(sql).unwrap().1;
		assert_eq!(out, Groups(Vec::new()));
		assert_eq!(sql, out.to_string());
	}

	#[test]
	fn limit_statement() {
		let sql = "LIMIT 100";
		let res = limit(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Limit(Value::from(100)));
		assert_eq!("LIMIT 100", format!("{}", out));
	}

	#[test]
	fn limit_statement_by() {
		let sql = "LIMIT BY 100";
		let res = limit(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(out, Limit(Value::from(100)));
		assert_eq!("LIMIT 100", format!("{}", out));
	}

	#[test]
	fn order_statement() {
		let sql = "ORDER field";
		let res = order(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: false,
				numeric: false,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY field", format!("{}", out));
	}

	#[test]
	fn order_statement_by() {
		let sql = "ORDER BY field";
		let res = order(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: false,
				numeric: false,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY field", format!("{}", out));
	}

	#[test]
	fn order_statement_random() {
		let sql = "ORDER RAND()";
		let res = order(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Default::default(),
				random: true,
				collate: false,
				numeric: false,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY RAND()", format!("{}", out));
	}

	#[test]
	fn order_statement_multiple() {
		let sql = "ORDER field, other.field";
		let res = order(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![
				Order {
					order: Idiom::parse("field"),
					random: false,
					collate: false,
					numeric: false,
					direction: true,
				},
				Order {
					order: Idiom::parse("other.field"),
					random: false,
					collate: false,
					numeric: false,
					direction: true,
				},
			])
		);
		assert_eq!("ORDER BY field, other.field", format!("{}", out));
	}

	#[test]
	fn order_statement_collate() {
		let sql = "ORDER field COLLATE";
		let res = order(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: true,
				numeric: false,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY field COLLATE", format!("{}", out));
	}

	#[test]
	fn order_statement_numeric() {
		let sql = "ORDER field NUMERIC";
		let res = order(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: false,
				numeric: true,
				direction: true,
			}])
		);
		assert_eq!("ORDER BY field NUMERIC", format!("{}", out));
	}

	#[test]
	fn order_statement_direction() {
		let sql = "ORDER field DESC";
		let res = order(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: false,
				numeric: false,
				direction: false,
			}])
		);
		assert_eq!("ORDER BY field DESC", format!("{}", out));
	}

	#[test]
	fn order_statement_all() {
		let sql = "ORDER field COLLATE NUMERIC DESC";
		let res = order(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Orders(vec![Order {
				order: Idiom::parse("field"),
				random: false,
				collate: true,
				numeric: true,
				direction: false,
			}])
		);
		assert_eq!("ORDER BY field COLLATE NUMERIC DESC", format!("{}", out));
	}

	#[test]
	fn output_statement() {
		let sql = "RETURN field, other.field";
		let res = output(sql);
		let out = res.unwrap().1;
		assert_eq!("RETURN field, other.field", format!("{}", out));
	}

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

	#[test]
	fn version_statement() {
		let sql = "VERSION '2020-01-01T00:00:00Z'";
		let res = version(sql);
		let out = res.unwrap().1;
		assert_eq!(out, Version(Datetime::try_from("2020-01-01T00:00:00Z").unwrap()));
		assert_eq!("VERSION '2020-01-01T00:00:00Z'", format!("{}", out));
	}
}
