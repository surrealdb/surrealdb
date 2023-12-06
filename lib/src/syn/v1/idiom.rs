use super::{
	comment::{mightbespace, shouldbespace},
	common::{
		closebracket, closeparentheses, commas, expect_delimited, openbracket, openparentheses,
	},
	ending,
	error::{expected, ExplainResultExt},
	literal::{ident, number, param, strand, table, tables},
	operator::dir,
	part::cond,
	value, IResult,
};
use crate::sql::{Cond, Fields, Graph, Idiom, Idioms, Part, Tables, Value};
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	character::complete::char,
	combinator::{self, cut, map, not, opt, peek},
	multi::{many0, many1, separated_list1},
	sequence::{preceded, terminated},
};

pub fn locals(i: &str) -> IResult<&str, Idioms> {
	let (i, v) = separated_list1(commas, local)(i)?;
	Ok((i, Idioms(v)))
}

pub fn graph(i: &str) -> IResult<&str, Graph> {
	let (i, dir) = dir(i)?;
	let (i, (what, cond, alias)) = alt((simple, custom))(i)?;
	Ok((
		i,
		Graph {
			dir,
			expr: Fields::all(),
			what,
			cond,
			alias,
			split: None,
			group: None,
			order: None,
			limit: None,
			start: None,
		},
	))
}

fn simple(i: &str) -> IResult<&str, (Tables, Option<Cond>, Option<Idiom>)> {
	let (i, w) = alt((any, one))(i)?;
	Ok((i, (w, None, None)))
}

fn custom(i: &str) -> IResult<&str, (Tables, Option<Cond>, Option<Idiom>)> {
	expect_delimited(
		openparentheses,
		|i| {
			let (i, w) = alt((any, tables))(i)?;
			let (i, c) = opt(|i| {
				let (i, _) = shouldbespace(i)?;
				let (i, v) = cond(i)?;
				Ok((i, v))
			})(i)?;
			let (i, a) = opt(|i| {
				let (i, _) = shouldbespace(i)?;
				let (i, _) = tag_no_case("AS")(i)?;
				let (i, _) = shouldbespace(i)?;
				let (i, v) = plain(i)?;
				Ok((i, v))
			})(i)?;
			Ok((i, (w, c, a)))
		},
		closeparentheses,
	)(i)
}

fn one(i: &str) -> IResult<&str, Tables> {
	let (i, v) = table(i)?;
	Ok((i, Tables::from(v)))
}

fn any(i: &str) -> IResult<&str, Tables> {
	map(char('?'), |_| Tables::default())(i)
}

/// Used in DEFINE FIELD and DEFINE INDEX clauses
pub fn local(i: &str) -> IResult<&str, Idiom> {
	expected("a local idiom", |i| {
		let (i, p) = first(i).explain("graphs are not allowed in a local idioms.", dir)?;
		let (i, mut v) = many0(local_part)(i)?;
		// Flatten is only allowed at the end
		let (i, flat) = opt(flatten)(i)?;
		if let Some(p) = flat {
			v.push(p);
		}
		v.insert(0, p);
		Ok((i, Idiom::from(v)))
	})(i)
}

/// Used in a SPLIT, ORDER, and GROUP clauses
///
/// Doesnt allow flatten, computed values or where selectors.
pub fn basic(i: &str) -> IResult<&str, Idiom> {
	use super::depth;
	// Limit recursion depth.
	let _diving = depth::dive(i)?;
	expected("a basic idiom", |i| {
		let (i, p) = first(i).explain("graphs are not allowed in a basic idioms.", dir)?;
		let (i, mut v) = many0(basic_part)(i)?;
		v.insert(0, p);
		Ok((i, Idiom::from(v)))
	})(i)
}

/// A simple idiom with one or more parts
pub fn plain(i: &str) -> IResult<&str, Idiom> {
	expected("a idiom", |i| {
		let (i, p) = alt((first, map(graph, Part::Graph)))(i)?;
		let (i, mut v) = many0(part)(i)?;
		v.insert(0, p);
		Ok((i, Idiom::from(v)))
	})(i)
}

/// Reparse a value which might part of an idiom.
pub fn reparse_idiom_start(start: Value, i: &str) -> IResult<&str, Value> {
	if start.can_start_idiom() {
		if let (i, Some(mut parts)) = opt(many1(part))(i)? {
			let start = Part::Start(start);
			parts.insert(0, start);
			let v = Value::from(Idiom::from(parts));
			return Ok((i, v));
		}
	}
	Ok((i, start))
}

/// A complex idiom with graph or many parts excluding idioms which start with a value.
pub fn multi_without_start(i: &str) -> IResult<&str, Idiom> {
	alt((
		|i| {
			let (i, p) = graph(i)?;
			let (i, mut v) = many0(part)(i)?;
			v.insert(0, Part::Graph(p));
			Ok((i, Idiom::from(v)))
		},
		|i| {
			let (i, p) = first(i)?;
			let (i, mut v) = many1(part)(i)?;
			v.insert(0, p);
			Ok((i, Idiom::from(v)))
		},
	))(i)
}

/// A simple field based idiom
pub fn path(i: &str) -> IResult<&str, Idiom> {
	let (i, p) = first(i)?;
	let (i, mut v) = many0(part)(i)?;
	v.insert(0, p);
	Ok((i, Idiom::from(v)))
}

/// A full complex idiom with any number of parts
#[cfg(test)]
pub fn idiom(i: &str) -> IResult<&str, Idiom> {
	use nom::combinator::fail;

	alt((
		plain,
		alt((multi_without_start, |i| {
			let (i, v) = value::value(i)?;
			let (i, v) = reparse_idiom_start(v, i)?;
			if let Value::Idiom(x) = v {
				return Ok((i, x));
			}
			fail(i)
		})),
	))(i)
}

pub fn part(i: &str) -> IResult<&str, Part> {
	alt((
		flatten,
		preceded(tag("."), cut(dot_part)),
		expect_delimited(openbracket, cut(bracketed_part), closebracket),
		map(graph, Part::Graph),
	))(i)
}

pub fn flatten(i: &str) -> IResult<&str, Part> {
	combinator::value(Part::Flatten, alt((tag("..."), tag("…"))))(i)
}

pub fn local_part(i: &str) -> IResult<&str, Part> {
	// Cant cut dot part since it might be part of the flatten at the end.
	alt((
		preceded(tag("."), dot_part),
		expect_delimited(openbracket, cut(local_bracketed_part), closebracket),
		// TODO explain
	))(i)
}

pub fn basic_part(i: &str) -> IResult<&str, Part> {
	alt((
		preceded(
			tag("."),
			cut(|i| dot_part(i).explain("flattening is not allowed with a basic idiom", tag(".."))),
		),
		|s| {
			let (i, _) = openbracket(s)?;
			let (i, v) = expected(
				"$, * or a number",
				cut(terminated(basic_bracketed_part, closebracket)),
			)(i)
			.explain("basic idioms don't allow computed values", bracketed_value)
			.explain("basic idioms don't allow where selectors", bracketed_where)?;
			Ok((i, v))
		},
	))(i)
}

fn dot_part(i: &str) -> IResult<&str, Part> {
	alt((
		combinator::value(Part::All, tag("*")),
		map(terminated(ident, ending::ident), Part::Field),
	))(i)
}

fn basic_bracketed_part(i: &str) -> IResult<&str, Part> {
	alt((
		combinator::value(Part::All, tag("*")),
		combinator::value(Part::Last, tag("$")),
		map(number, Part::Index),
	))(i)
}

fn local_bracketed_part(i: &str) -> IResult<&str, Part> {
	alt((combinator::value(Part::All, tag("*")), map(number, Part::Index)))(i)
		.explain("using `[$]` in a local idiom is not allowed", tag("$"))
}

fn bracketed_part(i: &str) -> IResult<&str, Part> {
	alt((
		combinator::value(Part::All, tag("*")),
		combinator::value(Part::Last, terminated(tag("$"), peek(closebracket))),
		map(number, Part::Index),
		bracketed_where,
		bracketed_value,
	))(i)
}

pub fn first(i: &str) -> IResult<&str, Part> {
	let (i, _) = peek(not(number))(i)?;
	let (i, v) = ident(i)?;
	let (i, _) = ending::ident(i)?;
	Ok((i, Part::Field(v)))
}

pub fn bracketed_where(i: &str) -> IResult<&str, Part> {
	let (i, _) = alt((
		terminated(tag("?"), mightbespace),
		terminated(tag_no_case("WHERE"), shouldbespace),
	))(i)?;

	let (i, v) = value::value(i)?;
	Ok((i, Part::Where(v)))
}

pub fn bracketed_value(i: &str) -> IResult<&str, Part> {
	let (i, v) =
		alt((map(strand, Value::Strand), map(param, Value::Param), map(basic, Value::Idiom)))(i)?;
	Ok((i, Part::Value(v)))
}

#[cfg(test)]
mod tests {
	use crate::sql::{Dir, Expression, Id, Number, Param, Strand, Table, Thing};
	use crate::syn::Parse;

	use super::*;

	#[test]
	fn graph_in() {
		let sql = "<-likes";
		let res = graph(sql);
		let out = res.unwrap().1;
		assert_eq!("<-likes", format!("{}", out));
	}

	#[test]
	fn graph_out() {
		let sql = "->likes";
		let res = graph(sql);
		let out = res.unwrap().1;
		assert_eq!("->likes", format!("{}", out));
	}

	#[test]
	fn graph_both() {
		let sql = "<->likes";
		let res = graph(sql);
		let out = res.unwrap().1;
		assert_eq!("<->likes", format!("{}", out));
	}

	#[test]
	fn graph_multiple() {
		let sql = "->(likes, follows)";
		let res = graph(sql);
		let out = res.unwrap().1;
		assert_eq!("->(likes, follows)", format!("{}", out));
	}

	#[test]
	fn graph_aliases() {
		let sql = "->(likes, follows AS connections)";
		let res = graph(sql);
		let out = res.unwrap().1;
		assert_eq!("->(likes, follows AS connections)", format!("{}", out));
	}

	#[test]
	fn graph_conditions() {
		let sql = "->(likes, follows WHERE influencer = true)";
		let res = graph(sql);
		let out = res.unwrap().1;
		assert_eq!("->(likes, follows WHERE influencer = true)", format!("{}", out));
	}

	#[test]
	fn graph_conditions_aliases() {
		let sql = "->(likes, follows WHERE influencer = true AS connections)";
		let res = graph(sql);
		let out = res.unwrap().1;
		assert_eq!("->(likes, follows WHERE influencer = true AS connections)", format!("{}", out));
	}

	#[test]
	fn idiom_number() {
		let sql = "13.495";
		let res = idiom(sql);
		assert!(res.is_err());
	}

	#[test]
	fn idiom_normal() {
		let sql = "test";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test")]));
	}

	#[test]
	fn idiom_quoted_backtick() {
		let sql = "`test`";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test")]));
	}

	#[test]
	fn idiom_quoted_brackets() {
		let sql = "⟨test⟩";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test")]));
	}

	#[test]
	fn idiom_nested() {
		let sql = "test.temp";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("temp")]));
	}

	#[test]
	fn idiom_nested_quoted() {
		let sql = "test.`some key`";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.`some key`", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("some key")]));
	}

	#[test]
	fn idiom_nested_array_all() {
		let sql = "test.temp[*]";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp[*]", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("temp"), Part::All]));
	}

	#[test]
	fn idiom_nested_array_last() {
		let sql = "test.temp[$]";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp[$]", format!("{}", out));
		assert_eq!(out, Idiom(vec![Part::from("test"), Part::from("temp"), Part::Last]));
	}

	#[test]
	fn idiom_nested_array_value() {
		let sql = "test.temp[*].text";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp[*].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![Part::from("test"), Part::from("temp"), Part::All, Part::from("text")])
		);
	}

	#[test]
	fn idiom_nested_array_question() {
		let sql = "test.temp[? test = true].text";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::from("test"),
				Part::from("temp"),
				Part::Where(Value::from(Expression::parse("test = true"))),
				Part::from("text")
			])
		);
	}

	#[test]
	fn idiom_nested_array_condition() {
		let sql = "test.temp[WHERE test = true].text";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::from("test"),
				Part::from("temp"),
				Part::Where(Value::from(Expression::parse("test = true"))),
				Part::from("text")
			])
		);
	}

	#[test]
	fn idiom_start_param_local_field() {
		let sql = "$test.temporary[0].embedded…";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("$test.temporary[0].embedded…", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::Start(Param::from("test").into()),
				Part::from("temporary"),
				Part::Index(Number::Int(0)),
				Part::from("embedded"),
				Part::Flatten,
			])
		);
	}

	#[test]
	fn idiom_start_thing_remote_traversal() {
		let sql = "person:test.friend->like->person";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!("person:test.friend->like->person", format!("{}", out));
		assert_eq!(
			out,
			Idiom(vec![
				Part::Start(Thing::from(("person", "test")).into()),
				Part::from("friend"),
				Part::Graph(Graph {
					dir: Dir::Out,
					expr: Fields::all(),
					what: Table::from("like").into(),
					cond: None,
					alias: None,
					split: None,
					group: None,
					order: None,
					limit: None,
					start: None,
				}),
				Part::Graph(Graph {
					dir: Dir::Out,
					expr: Fields::all(),
					what: Table::from("person").into(),
					cond: None,
					alias: None,
					split: None,
					group: None,
					order: None,
					limit: None,
					start: None,
				}),
			])
		);
	}

	#[test]
	fn part_all() {
		let sql = "[*]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[*]", format!("{}", out));
		assert_eq!(out, Part::All);
	}

	#[test]
	fn part_last() {
		let sql = "[$]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[$]", format!("{}", out));
		assert_eq!(out, Part::Last);
	}

	#[test]
	fn part_param() {
		let sql = "[$param]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[$param]", format!("{}", out));
		assert_eq!(out, Part::Value(Value::Param(Param::from("param"))));
	}

	#[test]
	fn part_flatten() {
		let sql = "...";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("…", format!("{}", out));
		assert_eq!(out, Part::Flatten);
	}

	#[test]
	fn part_flatten_ellipsis() {
		let sql = "…";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("…", format!("{}", out));
		assert_eq!(out, Part::Flatten);
	}

	#[test]
	fn part_number() {
		let sql = "[0]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[0]", format!("{}", out));
		assert_eq!(out, Part::Index(Number::from(0)));
	}

	#[test]
	fn part_expression_question() {
		let sql = "[?test = true]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[WHERE test = true]", format!("{}", out));
		assert_eq!(out, Part::Where(Value::from(Expression::parse("test = true"))));
	}

	#[test]
	fn part_expression_condition() {
		let sql = "[WHERE test = true]";
		let res = part(sql);
		let out = res.unwrap().1;
		assert_eq!("[WHERE test = true]", format!("{}", out));
		assert_eq!(out, Part::Where(Value::from(Expression::parse("test = true"))));
	}

	#[test]
	fn idiom_thing_number() {
		let sql = "test:1.foo";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Idiom(vec![
				Part::Start(Value::Thing(Thing {
					tb: "test".to_owned(),
					id: Id::Number(1),
				})),
				Part::from("foo"),
			])
		);
	}

	#[test]
	fn idiom_thing_index() {
		let sql = "test:1['foo']";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Idiom(vec![
				Part::Start(Value::Thing(Thing {
					tb: "test".to_owned(),
					id: Id::Number(1),
				})),
				Part::Value(Value::Strand(Strand("foo".to_owned()))),
			])
		);
	}

	#[test]
	fn idiom_thing_all() {
		let sql = "test:1.*";
		let res = idiom(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			Idiom(vec![
				Part::Start(Value::Thing(Thing {
					tb: "test".to_owned(),
					id: Id::Number(1),
				})),
				Part::All
			])
		);
	}
}
