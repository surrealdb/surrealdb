use super::{
	block::block,
	builtin,
	builtin::builtin_name,
	comment::mightbespace,
	common::{
		closebraces, commas, delimited_list0, expect_terminator, openbraces, openbracket, val_char,
	},
	depth,
	ending::keyword,
	error::expected,
	expression::{augment, cast, future, unary},
	function::{builtin_function, defined_function, model},
	idiom::{self, reparse_idiom_start},
	literal::{
		datetime::datetime, duration::duration, number, param, range::range, regex, strand::strand,
		table, uuid,
	},
	operator,
	part::edges,
	subquery::subquery,
	thing::thing,
	IResult,
};
use crate::sql::{Array, Expression, Idiom, Object, Table, Value, Values};
use nom::{
	branch::alt,
	bytes::complete::{is_not, tag, tag_no_case, take_while1},
	character::complete::char,
	combinator::{self, cut, into, opt},
	multi::{separated_list0, separated_list1},
	sequence::{delimited, terminated},
	Err,
};
use std::collections::BTreeMap;

mod geometry;
mod mock;

pub use geometry::geometry;
pub use mock::mock;

pub fn values(i: &str) -> IResult<&str, Values> {
	let (i, v) = separated_list1(commas, value)(i)?;
	Ok((i, Values(v)))
}

pub fn selects(i: &str) -> IResult<&str, Values> {
	let (i, v) = separated_list1(commas, select)(i)?;
	Ok((i, Values(v)))
}

pub fn whats(i: &str) -> IResult<&str, Values> {
	let (i, v) = separated_list1(commas, what)(i)?;
	Ok((i, Values(v)))
}

/// Parse any `Value` including expressions
pub fn value(i: &str) -> IResult<&str, Value> {
	let (i, start) = single(i)?;
	if let (i, Some(o)) = opt(operator::binary)(i)? {
		let _diving = depth::dive(i)?;
		let (i, r) = cut(value)(i)?;
		let expr = match r {
			Value::Expression(r) => augment(*r, start, o),
			_ => Expression::new(start, o, r),
		};
		let v = Value::from(expr);
		Ok((i, v))
	} else {
		Ok((i, start))
	}
}

/// Parse any `Value` excluding binary expressions
pub fn single(i: &str) -> IResult<&str, Value> {
	// Dive in `single` (as opposed to `value`) since it is directly
	// called by `Cast`
	let _diving = depth::dive(i)?;
	let (i, v) = alt((
		alt((
			terminated(
				alt((
					combinator::value(Value::None, tag_no_case("NONE")),
					combinator::value(Value::Null, tag_no_case("NULL")),
					combinator::value(Value::Bool(true), tag_no_case("true")),
					combinator::value(Value::Bool(false), tag_no_case("false")),
				)),
				keyword,
			),
			into(idiom::multi_without_start),
		)),
		alt((
			into(future),
			into(cast),
			path_like,
			into(geometry),
			into(subquery),
			into(datetime),
			into(duration),
			into(uuid),
			into(number),
			into(unary),
			into(object),
			into(array),
		)),
		alt((
			into(block),
			into(param),
			into(regex),
			into(mock),
			into(edges),
			into(range),
			into(thing),
			into(strand),
			into(idiom::path),
		)),
	))(i)?;
	reparse_idiom_start(v, i)
}

pub fn select_start(i: &str) -> IResult<&str, Value> {
	let (i, v) = alt((
		alt((
			into(unary),
			combinator::value(Value::None, tag_no_case("NONE")),
			combinator::value(Value::Null, tag_no_case("NULL")),
			combinator::value(Value::Bool(true), tag_no_case("true")),
			combinator::value(Value::Bool(false), tag_no_case("false")),
			into(idiom::multi_without_start),
		)),
		alt((
			into(future),
			into(cast),
			path_like,
			into(geometry),
			into(subquery),
			into(datetime),
			into(duration),
			into(uuid),
			into(number),
			into(object),
			into(array),
			into(block),
			into(param),
			into(regex),
			into(mock),
			into(edges),
			into(range),
			into(thing),
			into(table),
			into(strand),
		)),
	))(i)?;
	reparse_idiom_start(v, i)
}

/// A path like production: Constants, predefined functions, user defined functions and ml models.
pub fn path_like(i: &str) -> IResult<&str, Value> {
	alt((into(defined_function), into(model), |i| {
		let (i, v) = builtin_name(i)?;
		match v {
			builtin::BuiltinName::Constant(x) => Ok((i, x.into())),
			builtin::BuiltinName::Function(name) => {
				builtin_function(name, i).map(|(i, v)| (i, v.into()))
			}
		}
	}))(i)
}

pub fn select(i: &str) -> IResult<&str, Value> {
	let _diving = depth::dive(i)?;
	let (i, start) = select_start(i)?;
	if let (i, Some(op)) = opt(operator::binary)(i)? {
		// In a binary expression single ident's arent tables but paths.
		let start = match start {
			Value::Table(Table(x)) => Value::Idiom(Idiom::from(x)),
			x => x,
		};
		let (i, r) = cut(value)(i)?;
		let expr = match r {
			Value::Expression(r) => augment(*r, start, op),
			_ => Expression::new(start, op, r),
		};
		let v = Value::from(expr);
		Ok((i, v))
	} else {
		Ok((i, start))
	}
}

/// Used in CREATE, UPDATE, and DELETE clauses
pub fn what(i: &str) -> IResult<&str, Value> {
	let _diving = depth::dive(i)?;
	let (i, v) = alt((
		into(idiom::multi_without_start),
		path_like,
		into(subquery),
		into(datetime),
		into(duration),
		into(future),
		into(block),
		into(param),
		into(mock),
		into(edges),
		into(range),
		into(thing),
		into(table),
	))(i)?;
	reparse_idiom_start(v, i)
}

/// Used to parse any simple JSON-like value
pub fn json(i: &str) -> IResult<&str, Value> {
	let _diving = depth::dive(i)?;
	// Use a specific parser for JSON objects
	fn object(i: &str) -> IResult<&str, Object> {
		let (i, _) = char('{')(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = separated_list0(commas, |i| {
			let (i, k) = key(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = char(':')(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, v) = json(i)?;
			Ok((i, (String::from(k), v)))
		})(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = opt(char(','))(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char('}')(i)?;
		Ok((i, Object(v.into_iter().collect(), vec![])))
	}
	// Use a specific parser for JSON arrays
	fn array(i: &str) -> IResult<&str, Array> {
		let (i, _) = char('[')(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = separated_list0(commas, json)(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = opt(char(','))(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char(']')(i)?;
		Ok((i, Array(v)))
	}
	// Parse any simple JSON-like value
	alt((
		combinator::value(Value::Null, tag_no_case("null".as_bytes())),
		combinator::value(Value::Bool(true), tag_no_case("true".as_bytes())),
		combinator::value(Value::Bool(false), tag_no_case("false".as_bytes())),
		into(datetime),
		into(geometry),
		into(uuid),
		into(number),
		into(object),
		into(array),
		into(thing),
		into(strand),
	))(i)
}

pub fn array(i: &str) -> IResult<&str, Array> {
	fn entry_value(i: &str) -> IResult<&str, Value> {
		let (i, v) = cut(value)(i)?;
		Ok((i, v))
	}

	fn entry_spread(i: &str) -> IResult<&str, Value> {
		let (i, _) = tag("...")(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = cut(value)(i)?;
		Ok((i, Value::Spread(Box::new(v))))
	}

	fn entry(i: &str) -> IResult<&str, Value> {
		alt((entry_spread, entry_value))(i)
	}

	let (i, v) =
		delimited_list0(openbracket, commas, terminated(entry, mightbespace), char(']'))(i)?;

	Ok((i, Array(v)))
}

enum ObjectEntry {
	Kv((String, Value)),
	Spread(Value),
}

pub fn object(i: &str) -> IResult<&str, Object> {
	fn entry_kv(i: &str) -> IResult<&str, ObjectEntry> {
		let (i, k) = key(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = expected("`:`", char(':'))(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = cut(value)(i)?;
		Ok((i, ObjectEntry::Kv((String::from(k), v))))
	}

	fn entry_spread(i: &str) -> IResult<&str, ObjectEntry> {
		let (i, _) = tag("...")(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = cut(value)(i)?;
		Ok((i, ObjectEntry::Spread(v)))
	}

	fn entry(i: &str) -> IResult<&str, ObjectEntry> {
		alt((entry_kv, entry_spread))(i)
	}

	let start = i;
	let (i, _) = openbraces(i)?;
	let (i, first) = match entry(i) {
		Ok(x) => x,
		Err(Err::Error(_)) => {
			let (i, _) = closebraces(i)?;
			return Ok((i, Object(BTreeMap::new(), vec![])));
		}
		Err(Err::Failure(x)) => return Err(Err::Failure(x)),
		Err(Err::Incomplete(x)) => return Err(Err::Incomplete(x)),
	};

	let mut tree = BTreeMap::new();
	let mut spreads = Vec::<Value>::new();
	match first {
		ObjectEntry::Kv((k, v)) => {
			tree.insert(k, v);
		}
		ObjectEntry::Spread(v) => {
			spreads.push(v);
		}
	}

	let mut input = i;
	while let (i, Some(_)) = opt(commas)(input)? {
		if let (i, Some(_)) = opt(closebraces)(i)? {
			return Ok((i, Object(tree, spreads)));
		}
		let (i, v) = cut(entry)(i)?;
		match v {
			ObjectEntry::Kv((k, v)) => {
				tree.insert(k, v);
			}
			ObjectEntry::Spread(v) => {
				spreads.push(v);
			}
		}
		input = i
	}
	let (i, _) = expect_terminator(start, closebraces)(input)?;
	Ok((i, Object(tree, spreads)))
}

pub fn key(i: &str) -> IResult<&str, &str> {
	alt((key_none, key_single, key_double))(i)
}

fn key_none(i: &str) -> IResult<&str, &str> {
	take_while1(val_char)(i)
}

fn key_single(i: &str) -> IResult<&str, &str> {
	delimited(char('\''), is_not("\'\0"), char('\''))(i)
}

fn key_double(i: &str) -> IResult<&str, &str> {
	delimited(char('\"'), is_not("\"\0"), char('\"'))(i)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::array::{Clump, Transpose, Uniq};

	#[test]
	fn object_normal() {
		let sql = "{one:1,two:2,tre:3}";
		let res = object(sql);
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3, two: 2 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn object_commas() {
		let sql = "{one:1,two:2,tre:3,}";
		let res = object(sql);
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3, two: 2 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn object_expression() {
		let sql = "{one:1,two:2,tre:3+1}";
		let res = object(sql);
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3 + 1, two: 2 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_empty() {
		let sql = "[]";
		let res = array(sql);
		let out = res.unwrap().1;
		assert_eq!("[]", format!("{}", out));
		assert_eq!(out.0.len(), 0);
	}

	#[test]
	fn array_normal() {
		let sql = "[1,2,3]";
		let res = array(sql);
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_commas() {
		let sql = "[1,2,3,]";
		let res = array(sql);
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_expression() {
		let sql = "[1,2,3+1]";
		let res = array(sql);
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3 + 1]", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn array_fnc_clump() {
		fn test(input_sql: &str, clump_size: usize, expected_result: &str) {
			let arr_result = array(input_sql);
			let arr = arr_result.unwrap().1;
			let clumped_arr = arr.clump(clump_size);
			assert_eq!(format!("{}", clumped_arr), expected_result);
		}

		test("[0, 1, 2, 3]", 2, "[[0, 1], [2, 3]]");
		test("[0, 1, 2, 3, 4, 5]", 3, "[[0, 1, 2], [3, 4, 5]]");
		test("[0, 1, 2]", 2, "[[0, 1], [2]]");
		test("[]", 2, "[]");
	}

	#[test]
	fn array_fnc_transpose() {
		fn test(input_sql: &str, expected_result: &str) {
			let arr_result = array(input_sql);
			let arr = arr_result.unwrap().1;
			let transposed_arr = arr.transpose();
			assert_eq!(format!("{}", transposed_arr), expected_result);
		}

		test("[[0, 1], [2, 3]]", "[[0, 2], [1, 3]]");
		test("[[0, 1], [2]]", "[[0, 2], [1]]");
		test("[[0, 1, 2], [true, false]]", "[[0, true], [1, false], [2]]");
		test("[[0, 1], [2, 3], [4, 5]]", "[[0, 2, 4], [1, 3, 5]]");
	}

	#[test]
	fn array_fnc_uniq_normal() {
		let sql = "[1,2,1,3,3,4]";
		let res = array(sql);
		let out = res.unwrap().1.uniq();
		assert_eq!("[1, 2, 3, 4]", format!("{}", out));
		assert_eq!(out.0.len(), 4);
	}

	#[test]
	fn parse_false_exponent_number() {
		let (_, v) = what("3e").unwrap();
		assert_eq!(v, Value::Table(Table("3e".to_owned())))
	}
}
