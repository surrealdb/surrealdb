use super::{
	comment::{mightbespace, shouldbespace},
	IResult,
};
use crate::sql::index::Distance;
use crate::sql::{Dir, Operator};
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	character::complete::{char, u32, u8},
	combinator::{cut, map, opt, value},
};

pub fn assigner(i: &str) -> IResult<&str, Operator> {
	alt((
		value(Operator::Equal, char('=')),
		value(Operator::Inc, tag("+=")),
		value(Operator::Dec, tag("-=")),
		value(Operator::Ext, tag("+?=")),
	))(i)
}

pub fn unary(i: &str) -> IResult<&str, Operator> {
	unary_symbols(i)
}

pub fn unary_symbols(i: &str) -> IResult<&str, Operator> {
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((value(Operator::Neg, tag("-")), value(Operator::Not, tag("!"))))(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

pub fn binary(i: &str) -> IResult<&str, Operator> {
	alt((binary_symbols, binary_phrases))(i)
}

pub fn binary_symbols(i: &str) -> IResult<&str, Operator> {
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		alt((
			value(Operator::Or, tag("||")),
			value(Operator::And, tag("&&")),
			value(Operator::Tco, tag("?:")),
			value(Operator::Nco, tag("??")),
		)),
		alt((
			value(Operator::Exact, tag("==")),
			value(Operator::NotEqual, tag("!=")),
			value(Operator::AllEqual, tag("*=")),
			value(Operator::AnyEqual, tag("?=")),
			value(Operator::Equal, char('=')),
		)),
		alt((
			value(Operator::NotLike, tag("!~")),
			value(Operator::AllLike, tag("*~")),
			value(Operator::AnyLike, tag("?~")),
			value(Operator::Like, char('~')),
			matches,
			knn,
		)),
		alt((
			value(Operator::LessThanOrEqual, tag("<=")),
			value(Operator::LessThan, char('<')),
			value(Operator::MoreThanOrEqual, tag(">=")),
			value(Operator::MoreThan, char('>')),
			knn,
		)),
		alt((
			value(Operator::Pow, tag("**")),
			value(Operator::Add, char('+')),
			value(Operator::Sub, char('-')),
			value(Operator::Mul, char('*')),
			value(Operator::Mul, char('×')),
			value(Operator::Mul, char('∙')),
			value(Operator::Div, char('/')),
			value(Operator::Div, char('÷')),
			value(Operator::Rem, char('%')),
		)),
		alt((
			value(Operator::Contain, char('∋')),
			value(Operator::NotContain, char('∌')),
			value(Operator::Inside, char('∈')),
			value(Operator::NotInside, char('∉')),
			value(Operator::ContainAll, char('⊇')),
			value(Operator::ContainAny, char('⊃')),
			value(Operator::ContainNone, char('⊅')),
			value(Operator::AllInside, char('⊆')),
			value(Operator::AnyInside, char('⊂')),
			value(Operator::NoneInside, char('⊄')),
		)),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

pub fn binary_phrases(i: &str) -> IResult<&str, Operator> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = alt((
		alt((
			value(Operator::Or, tag_no_case("OR")),
			value(Operator::And, tag_no_case("AND")),
			value(Operator::NotEqual, tag_no_case("IS NOT")),
			value(Operator::Equal, tag_no_case("IS")),
		)),
		alt((
			value(Operator::ContainAll, tag_no_case("CONTAINSALL")),
			value(Operator::ContainAny, tag_no_case("CONTAINSANY")),
			value(Operator::ContainNone, tag_no_case("CONTAINSNONE")),
			value(Operator::NotContain, tag_no_case("CONTAINSNOT")),
			value(Operator::Contain, tag_no_case("CONTAINS")),
			value(Operator::AllInside, tag_no_case("ALLINSIDE")),
			value(Operator::AnyInside, tag_no_case("ANYINSIDE")),
			value(Operator::NoneInside, tag_no_case("NONEINSIDE")),
			value(Operator::NotInside, tag_no_case("NOTINSIDE")),
			value(Operator::Inside, tag_no_case("INSIDE")),
			value(Operator::Outside, tag_no_case("OUTSIDE")),
			value(Operator::Intersects, tag_no_case("INTERSECTS")),
			value(Operator::NotInside, tag_no_case("NOT IN")),
			value(Operator::Inside, tag_no_case("IN")),
		)),
	))(i)?;
	let (i, _) = shouldbespace(i)?;
	Ok((i, v))
}

pub fn matches(i: &str) -> IResult<&str, Operator> {
	let (i, _) = char('@')(i)?;
	cut(|i| {
		let (i, reference) = opt(u8)(i)?;
		let (i, _) = char('@')(i)?;
		Ok((i, Operator::Matches(reference)))
	})(i)
}
pub fn minkowski(i: &str) -> IResult<&str, Distance> {
	let (i, _) = tag_no_case("MINKOWSKI")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, order) = u32(i)?;
	Ok((i, Distance::Minkowski(order.into())))
}
pub fn distance(i: &str) -> IResult<&str, Distance> {
	alt((
		map(tag_no_case("CHEBYSHEV"), |_| Distance::Chebyshev),
		map(tag_no_case("COSINE"), |_| Distance::Cosine),
		map(tag_no_case("EUCLIDEAN"), |_| Distance::Euclidean),
		map(tag_no_case("HAMMING"), |_| Distance::Hamming),
		map(tag_no_case("JACCARD"), |_| Distance::Jaccard),
		map(tag_no_case("MANHATTAN"), |_| Distance::Manhattan),
		minkowski,
		map(tag_no_case("PEARSON"), |_| Distance::Pearson),
	))(i)
}

fn knn_distance(i: &str) -> IResult<&str, Operator> {
	let (i, k) = u32(i)?;
	let (i, _) = char(',')(i)?;
	let (i, d) = distance(i)?;
	Ok((i, Operator::Knn(k, Some(d))))
}

fn ann(i: &str) -> IResult<&str, Operator> {
	let (i, k) = u32(i)?;
	let (i, _) = char(',')(i)?;
	let (i, ef) = u32(i)?;
	Ok((i, Operator::Ann(k, ef)))
}

fn knn_no_distance(i: &str) -> IResult<&str, Operator> {
	let (i, k) = u32(i)?;
	Ok((i, Operator::Knn(k, None)))
}

fn knn_ann(i: &str) -> IResult<&str, Operator> {
	alt((knn_distance, ann, knn_no_distance))(i)
}

pub fn knn(i: &str) -> IResult<&str, Operator> {
	alt((
		|i| {
			let (i, _) = opt(tag_no_case("knn"))(i)?;
			let (i, _) = char('<')(i)?;
			let (i, op) = knn_ann(i)?;
			let (i, _) = char('>')(i)?;
			Ok((i, op))
		},
		|i| {
			let (i, _) = tag("<|")(i)?;
			cut(|i| {
				let (i, op) = knn_ann(i)?;
				let (i, _) = tag("|>")(i)?;
				Ok((i, op))
			})(i)
		},
	))(i)
}

pub fn dir(i: &str) -> IResult<&str, Dir> {
	alt((value(Dir::Both, tag("<->")), value(Dir::In, tag("<-")), value(Dir::Out, tag("->"))))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

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
	fn matches_without_reference() {
		let res = matches("@@");
		let out = res.unwrap().1;
		assert_eq!("@@", format!("{}", out));
		assert_eq!(out, Operator::Matches(None));
	}

	#[test]
	fn matches_with_reference() {
		let res = matches("@12@");
		let out = res.unwrap().1;
		assert_eq!("@12@", format!("{}", out));
		assert_eq!(out, Operator::Matches(Some(12u8)));
	}

	#[test]
	fn matches_with_invalid_reference() {
		let res = matches("@256@");
		res.unwrap_err();
	}

	#[test]
	fn test_knn() {
		let res = knn("<5>");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<|5|>", format!("{}", out));
		assert_eq!(out, Operator::Knn(5, None));

		let res = knn("<|5|>");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<|5|>", format!("{}", out));
		assert_eq!(out, Operator::Knn(5, None));
	}

	#[test]
	fn test_knn_with_distance() {
		let res = knn("<3,EUCLIDEAN>");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<|3,EUCLIDEAN|>", format!("{}", out));
		assert_eq!(out, Operator::Knn(3, Some(Distance::Euclidean)));

		let res = knn("<|3,EUCLIDEAN|>");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<|3,EUCLIDEAN|>", format!("{}", out));
		assert_eq!(out, Operator::Knn(3, Some(Distance::Euclidean)));
	}

	#[test]
	fn test_ann() {
		let res = knn("<3,100>");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<|3,100|>", format!("{}", out));
		assert_eq!(out, Operator::Ann(3, 100));

		let res = knn("<|3,100|>");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<|3,100|>", format!("{}", out));
		assert_eq!(out, Operator::Ann(3, 100));
	}

	#[test]
	fn test_knn_with_prefix() {
		let res = knn("<|5|>");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<|5|>", format!("{}", out));
		assert_eq!(out, Operator::Knn(5, None));
	}
}
