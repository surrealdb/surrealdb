use reblessive::Stack;
use rust_decimal::Decimal;

use crate::sql::literal::ObjectEntry;
use crate::sql::{
	BinaryOperator, Constant, Expr, Ident, Idiom, Literal, Part, RecordIdKeyLit, RecordIdLit,
};
use crate::syn;
use crate::syn::parser::{Parser, ParserSettings};
use crate::val::Geometry;

#[test]
fn parse_index_expression() {
	let value = syn::parse_with("a[1 + 1]".as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	let Expr::Idiom(x) = value else {
		panic!("not the right value type");
	};
	assert_eq!(x.0[0], Part::Field(Ident::from_strand(strand!("a").to_owned())));
	assert_eq!(
		x.0[1],
		Part::Value(Expr::Binary {
			left: Box::new(Expr::Literal(Literal::Integer(1))),
			op: BinaryOperator::Add,
			right: Box::new(Expr::Literal(Literal::Integer(1))),
		})
	)
}

#[test]
fn parse_coordinate() {
	let coord = syn::parse_with("(1.88, -18.0)".as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	let Expr::Literal(Literal::Geometry(Geometry::Point(x))) = coord else {
		panic!("not the right value");
	};
	assert_eq!(x.x(), 1.88);
	assert_eq!(x.y(), -18.0);
}

#[test]
fn parse_numeric_object_key() {
	let v = syn::parse_with("{ 00: 0 }".as_bytes(), async |parser, stk| {
		parser.parse_expr_table(stk).await
	})
	.unwrap();
	let Expr::Literal(Literal::Object(object)) = v else {
		panic!("not an object");
	};
	assert!(object.len() == 1);
	assert_eq!(object[0].value, Expr::Literal(Literal::Integer(0)));
}

#[test]
fn parse_range_operator() {
	syn::parse_with("1..2".as_bytes(), async |parser, stk| parser.parse_expr_field(stk).await)
		.unwrap();
}

#[test]
fn parse_large_depth_object() {
	let mut text = String::new();
	let start = r#" { foo: "#;
	let middle = r#" {bar: 1} "#;
	let end = r#" } "#;

	for _ in 0..1000 {
		text.push_str(start);
	}
	text.push_str(middle);
	for _ in 0..1000 {
		text.push_str(end);
	}
	let mut parser = Parser::new_with_settings(
		text.as_bytes(),
		ParserSettings {
			query_recursion_limit: 100000,
			object_recursion_limit: 100000,
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	let query = stack.enter(|stk| parser.parse_expr_inherit(stk)).finish().unwrap();
	let Expr::Literal(Literal::Object(ref object)) = query else {
		panic!()
	};
	let mut object = object;
	for _ in 0..999 {
		let Some(Expr::Literal(Literal::Object(new_object))) =
			object.iter().find(|x| x.key == "foo").map(|x| &x.value)
		else {
			panic!()
		};
		object = new_object
	}
}

#[test]
fn parse_large_depth_record_id() {
	let mut text = String::new();
	let start = r#" r"a:[ "#;
	let middle = r#" b:{c: 1} "#;
	let end = r#" ]" "#;

	for _ in 0..1000 {
		text.push_str(start);
	}
	text.push_str(middle);
	for _ in 0..1000 {
		text.push_str(end);
	}
	let mut parser = Parser::new_with_settings(
		text.as_bytes(),
		ParserSettings {
			query_recursion_limit: 100000,
			object_recursion_limit: 100000,
			..Default::default()
		},
	);
	let mut stack = Stack::new();
	let query = stack.enter(|stk| parser.parse_expr_inherit(stk)).finish().unwrap();
	let Expr::Literal(Literal::RecordId(ref rid)) = query else {
		panic!()
	};
	let mut rid = rid;
	for _ in 0..999 {
		let RecordIdKeyLit::Array(ref x) = rid.key else {
			panic!()
		};
		let Expr::Literal(Literal::RecordId(ref new_rid)) = x[0] else {
			panic!()
		};
		rid = new_rid
	}
}

#[test]
fn parse_recursive_record_string() {
	let res = syn::parse_with(r#" r"a:[r"b:{c: r"d:1"}"]" "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Literal(Literal::RecordId(RecordIdLit {
			table: "a".to_owned(),
			key: RecordIdKeyLit::Array(vec![Expr::Literal(Literal::RecordId(RecordIdLit {
				table: "b".to_owned(),
				key: RecordIdKeyLit::Object(vec![ObjectEntry {
					key: "c".to_owned(),
					value: Expr::Literal(Literal::RecordId(RecordIdLit {
						table: "d".to_owned(),
						key: RecordIdKeyLit::Number(1)
					}))
				}])
			}))])
		}))
	)
}

#[test]
fn parse_record_string_2() {
	let res = syn::parse_with(r#" r'a:["foo"]' "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(
		res,
		Expr::Literal(Literal::RecordId(RecordIdLit {
			table: "a".to_owned(),
			key: RecordIdKeyLit::Array(vec![Expr::Literal(Literal::Strand(
				strand!("foo").to_owned()
			))])
		}))
	)
}

#[test]
fn parse_i64() {
	let res = syn::parse_with(r#" -9223372036854775808 "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(res, Expr::Literal(Literal::Integer(i64::MIN)));

	let res = syn::parse_with(r#" 9223372036854775807 "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(res, Expr::Literal(Literal::Integer(i64::MAX)));
}

#[test]
fn parse_decimal() {
	let res = syn::parse_with(r#" 0dec "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(res, Expr::Literal(Literal::Decimal(Decimal::ZERO)));
}

#[test]
fn constant_lowercase() {
	let out = syn::parse_with(r#" math::pi "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::MathPi));

	let out = syn::parse_with(r#" math::inf "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::MathInf));

	let out = syn::parse_with(r#" math::neg_inf "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::MathNegInf));

	let out = syn::parse_with(r#" time::epoch "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::TimeEpoch));
}

#[test]
fn constant_uppercase() {
	let out = syn::parse_with(r#" MATH::PI "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::MathPi));

	let out = syn::parse_with(r#" MATH::INF "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::MathInf));

	let out = syn::parse_with(r#" MATH::NEG_INF "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::MathNegInf));

	let out = syn::parse_with(r#" TIME::EPOCH "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::TimeEpoch));
}

#[test]
fn constant_mixedcase() {
	let out = syn::parse_with(r#" MaTh::Pi "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::MathPi));

	let out = syn::parse_with(r#" MaTh::Inf "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::MathInf));

	let out = syn::parse_with(r#" MaTh::Neg_Inf "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::MathNegInf));

	let out = syn::parse_with(r#" TiME::ePoCH "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert_eq!(out, Expr::Constant(Constant::TimeEpoch));
}

#[test]
fn scientific_decimal() {
	let res = syn::parse_with(r#" 9.7e-7dec "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert!(matches!(res, Expr::Literal(Literal::Decimal(_))));
	assert_eq!(res.to_string(), "0.00000097dec")
}

#[test]
fn scientific_number() {
	let res = syn::parse_with(r#" 9.7e-5"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	assert!(matches!(res, Expr::Literal(Literal::Float(_))));
	assert_eq!(res.to_string(), "0.000097f")
}

#[test]
fn number_method() {
	let res = syn::parse_with(r#" 9.7e-5.sin()"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	let expected = Expr::Idiom(Idiom(vec![
		Part::Start(Expr::Literal(Literal::Float(9.7e-5))),
		Part::Method("sin".to_string(), vec![]),
	]));
	assert_eq!(res, expected);

	let res = syn::parse_with(r#" 1.sin()"#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap();
	let expected = Expr::Idiom(Idiom(vec![
		Part::Start(Expr::Literal(Literal::Integer(1))),
		Part::Method("sin".to_string(), vec![]),
	]));
	assert_eq!(res, expected);
}

#[test]
fn datetime_error() {
	syn::parse_with(r#" d"2001-01-01T01:01:01.9999999999" "#.as_bytes(), async |parser, stk| {
		parser.parse_expr_field(stk).await
	})
	.unwrap_err();
}

#[test]
fn empty_string() {
	syn::parse_with("".as_bytes(), async |parser, stk| parser.parse_expr_field(stk).await)
		.unwrap_err();
}
