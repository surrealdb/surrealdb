use super::super::{
	comment::{mightbespace, shouldbespace},
	error::expected,
	literal::{param, table},
	part::{data, output, timeout},
	subquery::subquery,
	thing::thing,
	value::array,
	IResult,
};
use crate::sql::{statements::RelateStatement, Data, Value};
use nom::{
	branch::alt,
	bytes::complete::{tag, tag_no_case},
	combinator::{cut, into, opt, value},
	sequence::preceded,
};

pub fn relate(i: &str) -> IResult<&str, RelateStatement> {
	use super::super::depth;
	// Limit recursion depth.
	trace!("Starting relate for query: {}", i);
	let _diving = depth::dive(i)?;
	let (i, _) = tag_no_case("RELATE")(i)?;
	let (i, only) = opt(preceded(shouldbespace, tag_no_case("ONLY")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, path) = relate_oi(i)?;
	warn!("outside relate_oi");
	let (i, uniq) = opt(preceded(shouldbespace, tag_no_case("UNIQUE")))(i)?;
	warn!("handled unique");
	let (i, content) = opt(preceded(shouldbespace, content))(i)?;
	warn!("handled content");
	let (i, data) = opt(preceded(shouldbespace, data))(i)?;
	let data = content.or(data);
	warn!("handled data");
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	warn!("handled output");
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	warn!("handled timeout");
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	warn!("handled parallel");
	trace!("Finished relate for query: {}", i);
	Ok((
		i,
		RelateStatement {
			only: only.is_some(),
			kind: path.0,
			from: path.1,
			with: path.2,
			uniq: uniq.is_some(),
			data,
			output,
			timeout,
			parallel: parallel.is_some(),
		},
	))
}

fn relate_oi(i: &str) -> IResult<&str, (Value, Value, Value)> {
	warn!("Starting relate_oi for {}", i);
	let (i, prefix) = alt((into(subquery), into(array), into(param), into(thing)))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, is_o) =
		expected("`->` or `<-`", cut(alt((value(true, tag("->")), value(false, tag("<-"))))))(i)?;

	warn!("Finished relate_oi: {}", i);
	if is_o {
		let (i, (kind, with)) = cut(relate_o)(i)?;
		Ok((i, (kind, prefix, with)))
	} else {
		let (i, (kind, from)) = cut(relate_i)(i)?;
		Ok((i, (kind, from, prefix)))
	}
}

fn relate_o(i: &str) -> IResult<&str, (Value, Value)> {
	warn!("Starting relate_o for {}", i);
	let (i, _) = mightbespace(i)?;
	warn!("After space one");
	let (i, kind) = alt((into(thing), into(table), into(param)))(i)?;
	warn!("After kind");
	let (i, _) = mightbespace(i)?;
	warn!("After space two");
	let (i, _) = tag("->")(i)?;
	warn!("After arrow");
	let (i, _) = mightbespace(i)?;
	warn!("After space three");
	let res: IResult<&str, Value> = alt((into(subquery), into(array), into(param), into(thing)))(i);
	if let Err(e) = res {
		warn!("Error in relate_o: {:?}", e);
	}
	warn!("After final");
	let (i, with) = alt((into(subquery), into(array), into(param), into(thing)))(i)?;
	warn!("Finished relate_o for {}", i);
	Ok((i, (kind, with)))
}

fn relate_i(i: &str) -> IResult<&str, (Value, Value)> {
	warn!("Starting relate_i for {}", i);
	let (i, _) = mightbespace(i)?;
	let (i, kind) = alt((into(thing), into(table)))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("<-")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, from) = alt((into(subquery), into(array), into(param), into(thing)))(i)?;
	warn!("Finished relate_i for {}", i);
	Ok((i, (kind, from)))
}

fn content(i: &str) -> IResult<&str, Data> {
	let (i, _) = tag_no_case("CONTENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = alt((into(array), into(param)))(i)?;
	Ok((i, Data::ContentExpression(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn relate_statement_in() {
		let sql = "RELATE animal:koala<-like<-person:tobie";
		let res = relate(sql);
		let out = res.unwrap().1;
		assert_eq!("RELATE person:tobie -> like -> animal:koala", format!("{}", out))
	}

	#[test]
	fn relate_statement_out() {
		let sql = "RELATE person:tobie->like->animal:koala";
		let res = relate(sql);
		let out = res.unwrap().1;
		assert_eq!("RELATE person:tobie -> like -> animal:koala", format!("{}", out))
	}

	#[test]
	fn relate_statement_params() {
		let sql = "RELATE $tobie->like->$koala";
		let res = relate(sql);
		let out = res.unwrap().1;
		assert_eq!("RELATE $tobie -> like -> $koala", format!("{}", out))
	}

	#[test]
	fn relate_statement_content() {
		let sql = "RELATE $tobie->like->$koala CONTENT $bla";
		let res = relate(sql);
		let out = res.unwrap().1;
		assert_eq!("RELATE $tobie -> like -> $koala CONTENT $bla", format!("{}", out));
		assert_eq!(
			out,
			RelateStatement {
				only: false,
				kind: Value::Param(Param(Ident("$koala".to_owned()))),
				from: Value::Param(Param(Ident("$tobie".to_owned()))),
				with: Value::Param(Param(Ident("$bla".to_owned()))),
				uniq: false,
				data: None,
				output: None,
				timeout: None,
				parallel: false,
			}
		)
	}
}
