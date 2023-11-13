use super::{
	comment::{block, mightbespace, slash},
	common::{
		closechevron, closeparentheses, commas, delimited_list0, delimited_list1, expect_delimited,
		openchevron, openparentheses, val_char,
	},
	depth,
	error::{expect_tag_no_case, expected},
	value::value,
	IResult,
};
use crate::sql::{Function, Model, Script};
use nom::{
	branch::alt,
	bytes::complete::{escaped, is_not, tag, take_while1},
	character::complete::{anychar, char, i64, multispace0},
	combinator::{cut, recognize},
	multi::{many0, many1, separated_list1},
	sequence::{delimited, terminated},
};

const SINGLE: char = '\'';
const SINGLE_ESC_NUL: &str = "'\\\0";

const DOUBLE: char = '"';
const DOUBLE_ESC_NUL: &str = "\"\\\0";

const BACKTICK: char = '`';
const BACKTICK_ESC_NUL: &str = "`\\\0";

const OBJECT_BEG: char = '{';
const OBJECT_END: char = '}';

pub fn defined_function(i: &str) -> IResult<&str, Function> {
	alt((custom, script))(i)
}

pub fn builtin_function<'a>(name: &'a str, i: &'a str) -> IResult<&'a str, Function> {
	let (i, a) = expected(
		"function arguments",
		delimited_list0(openparentheses, commas, terminated(cut(value), mightbespace), char(')')),
	)(i)?;
	Ok((i, Function::Normal(name.to_string(), a)))
}

pub fn custom(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("fn::")(i)?;
	cut(|i| {
		let (i, s) = recognize(separated_list1(tag("::"), take_while1(val_char)))(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, a) = expected(
			"function arguments",
			delimited_list0(
				cut(openparentheses),
				commas,
				terminated(cut(value), mightbespace),
				char(')'),
			),
		)(i)?;
		Ok((i, Function::Custom(s.to_string(), a)))
	})(i)
}

fn script(i: &str) -> IResult<&str, Function> {
	let (i, _) = tag("function")(i)?;
	cut(|i| {
		let (i, _) = mightbespace(i)?;
		let (i, a) = delimited_list0(
			openparentheses,
			commas,
			terminated(cut(value), mightbespace),
			char(')'),
		)(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char('{')(i)?;
		let (i, v) = script_body(i)?;
		let (i, _) = char('}')(i)?;
		Ok((i, Function::Script(v, a)))
	})(i)
}

pub fn model(i: &str) -> IResult<&str, Model> {
	let (i, _) = tag("ml::")(i)?;

	cut(|i| {
		let (i, name) = recognize(separated_list1(tag("::"), take_while1(val_char)))(i)?;

		let (i, version) =
			expected("a version", expect_delimited(openchevron, version, closechevron))(i)?;

		let (i, args) = expected(
			"model arguments",
			delimited_list1(openparentheses, commas, value, closeparentheses),
		)(i)?;

		Ok((
			i,
			Model {
				name: name.to_owned(),
				version,
				args,
			},
		))
	})(i)
}

pub fn version(i: &str) -> IResult<&str, String> {
	use std::fmt::Write;

	let (i, major) = expected("a version number", i64)(i)?;
	let (i, _) = expect_tag_no_case(".")(i)?;
	let (i, minor) = expected("a version number", i64)(i)?;
	let (i, _) = expect_tag_no_case(".")(i)?;
	let (i, patch) = expected("a version number", i64)(i)?;

	let mut res = String::new();
	// Writing into a string can never error.
	write!(&mut res, "{major}.{minor}.{patch}").unwrap();
	Ok((i, res))
}

pub fn script_body(i: &str) -> IResult<&str, Script> {
	let (i, v) = script_body_raw(i)?;
	Ok((i, Script(String::from(v))))
}

fn script_body_raw(i: &str) -> IResult<&str, &str> {
	let _diving = depth::dive(i)?;
	recognize(many0(alt((
		script_body_comment,
		script_body_object,
		script_body_string,
		script_body_maths,
		script_body_other,
	))))(i)
}

fn script_body_maths(i: &str) -> IResult<&str, &str> {
	recognize(tag("/"))(i)
}

fn script_body_other(i: &str) -> IResult<&str, &str> {
	recognize(many1(is_not("/{}`'\"")))(i)
}

fn script_body_comment(i: &str) -> IResult<&str, &str> {
	recognize(delimited(multispace0, many1(alt((block, slash))), multispace0))(i)
}

fn script_body_object(i: &str) -> IResult<&str, &str> {
	recognize(delimited(char(OBJECT_BEG), script_body_raw, char(OBJECT_END)))(i)
}

fn script_body_string(i: &str) -> IResult<&str, &str> {
	recognize(alt((
		|i| {
			let (i, _) = char(SINGLE)(i)?;
			let (i, _) = char(SINGLE)(i)?;
			Ok((i, ""))
		},
		|i| {
			let (i, _) = char(DOUBLE)(i)?;
			let (i, _) = char(DOUBLE)(i)?;
			Ok((i, ""))
		},
		|i| {
			let (i, _) = char(SINGLE)(i)?;
			let (i, v) = escaped(is_not(SINGLE_ESC_NUL), '\\', anychar)(i)?;
			let (i, _) = char(SINGLE)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, _) = char(DOUBLE)(i)?;
			let (i, v) = escaped(is_not(DOUBLE_ESC_NUL), '\\', anychar)(i)?;
			let (i, _) = char(DOUBLE)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, _) = char(BACKTICK)(i)?;
			let (i, v) = escaped(is_not(BACKTICK_ESC_NUL), '\\', anychar)(i)?;
			let (i, _) = char(BACKTICK)(i)?;
			Ok((i, v))
		},
	)))(i)
}

#[cfg(test)]
mod tests {
	use super::super::builtin::{builtin_name, BuiltinName};
	use super::*;
	use crate::sql::Value;
	use crate::syn::{self, test::Parse};

	fn function(i: &str) -> IResult<&str, Function> {
		alt((defined_function, |i| {
			let (i, name) = builtin_name(i)?;
			let BuiltinName::Function(x) = name else {
				panic!("not a function")
			};
			builtin_function(x, i)
		}))(i)
	}

	#[test]
	fn function_single() {
		let sql = "count()";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("count()", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("count"), vec![]));
	}

	#[test]
	fn function_single_not() {
		let sql = "not(10)";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("not(10)", format!("{}", out));
		assert_eq!(out, Function::Normal("not".to_owned(), vec![10.into()]));
	}

	#[test]
	fn function_module() {
		let sql = "rand::uuid()";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("rand::uuid()", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("rand::uuid"), vec![]));
	}

	#[test]
	fn function_arguments() {
		let sql = "string::is::numeric(null)";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("string::is::numeric(NULL)", format!("{}", out));
		assert_eq!(out, Function::Normal(String::from("string::is::numeric"), vec![Value::Null]));
	}

	#[test]
	fn function_simple_together() {
		let sql = "function() { return 'test'; }";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("function() { return 'test'; }", format!("{}", out));
		assert_eq!(out, Function::Script(Script::parse(" return 'test'; "), vec![]));
	}

	#[test]
	fn function_simple_whitespace() {
		let sql = "function () { return 'test'; }";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!("function() { return 'test'; }", format!("{}", out));
		assert_eq!(out, Function::Script(Script::parse(" return 'test'; "), vec![]));
	}

	#[test]
	fn function_script_expression() {
		let sql = "function() { return this.tags.filter(t => { return t.length > 3; }); }";
		let res = function(sql);
		let out = res.unwrap().1;
		assert_eq!(
			"function() { return this.tags.filter(t => { return t.length > 3; }); }",
			format!("{}", out)
		);
		assert_eq!(
			out,
			Function::Script(
				Script::parse(" return this.tags.filter(t => { return t.length > 3; }); "),
				vec![]
			)
		);
	}

	#[test]
	fn ml_model_example() {
		let sql = r#"ml::insurance::prediction<1.0.0>({
				age: 18,
				disposable_income: "yes",
				purchased_before: true
			})
		"#;
		let res = model(sql);
		let out = res.unwrap().1.to_string();
		assert_eq!("ml::insurance::prediction<1.0.0>({ age: 18, disposable_income: 'yes', purchased_before: true })",out);
	}

	#[test]
	fn ml_model_example_in_select() {
		let sql = r"
			SELECT
			name,
			age,
			ml::insurance::prediction<1.0.0>({
				age: age,
				disposable_income: math::round(income),
				purchased_before: array::len(->purchased->property) > 0,
			}) AS likely_to_buy FROM person:tobie;
		";
		let res = syn::parse(sql);
		let out = res.unwrap().to_string();
		assert_eq!(
			"SELECT name, age, ml::insurance::prediction<1.0.0>({ age: age, disposable_income: math::round(income), purchased_before: array::len(->purchased->property) > 0 }) AS likely_to_buy FROM person:tobie;",
			out,
		);
	}

	#[test]
	fn ml_model_with_mutiple_arguments() {
		let sql = "ml::insurance::prediction<1.0.0>(1,2,3,4,);";
		let res = syn::parse(sql);
		let out = res.unwrap().to_string();
		assert_eq!("ml::insurance::prediction<1.0.0>(1,2,3,4);", out,);
	}

	#[test]
	fn script_basic() {
		let sql = "return true;";
		let res = script_body(sql);
		let out = res.unwrap().1;
		assert_eq!("return true;", format!("{}", out));
		assert_eq!(out, Script::from("return true;"));
	}

	#[test]
	fn script_object() {
		let sql = "return { test: true, something: { other: true } };";
		let res = script_body(sql);
		let out = res.unwrap().1;
		assert_eq!("return { test: true, something: { other: true } };", format!("{}", out));
		assert_eq!(out, Script::from("return { test: true, something: { other: true } };"));
	}

	#[test]
	fn script_closure() {
		let sql = "return this.values.map(v => `This value is ${Number(v * 3)}`);";
		let res = script_body(sql);
		let out = res.unwrap().1;
		assert_eq!(
			"return this.values.map(v => `This value is ${Number(v * 3)}`);",
			format!("{}", out)
		);
		assert_eq!(
			out,
			Script::from("return this.values.map(v => `This value is ${Number(v * 3)}`);")
		);
	}

	#[test]
	fn script_complex() {
		let sql = r#"return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };"#;
		let res = script_body(sql);
		let out = res.unwrap().1;
		assert_eq!(
			r#"return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };"#,
			format!("{}", out)
		);
		assert_eq!(
			out,
			Script::from(
				r#"return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };"#
			)
		);
	}

	#[test]
	fn script_advanced() {
		let sql = r#"
			// {
			// }
			// {}
			// { }
			/* { */
			/* } */
			/* {} */
			/* { } */
			/* {{{ $ }} */
			/* /* /* /* */
			let x = {};
			let x = { };
			let x = '{';
			let x = "{";
			let x = '}';
			let x = "}";
			let x = '} } { {';
			let x = 3 / 4 * 2;
			let x = /* something */ 45 + 2;
		"#;
		let res = script_body(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
		assert_eq!(out, Script::from(sql));
	}
}
