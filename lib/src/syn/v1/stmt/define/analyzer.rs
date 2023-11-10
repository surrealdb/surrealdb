use super::super::super::{
	block::block,
	comment::{mightbespace, shouldbespace},
	common::{closeparentheses, commas, commasorspace, openparentheses},
	ending,
	error::{expect_tag_no_case, expected, ExplainResultExt},
	idiom::{basic, plain},
	literal::{
		datetime, duration, filters, ident, param, scoring, strand, table, tables, timeout,
		tokenizer::tokenizers,
	},
	operator::{assigner, dir},
	part::{
		cond, data,
		data::{single, update},
		output,
	},
	thing::thing,
	value::{value, values, whats},
	IResult,
};
use crate::sql::{filter::Filter, statements::DefineAnalyzerStatement, Strand, Tokenizer, Value};
use nom::{
	branch::alt,
	bytes::complete::{escaped, escaped_transform, is_not, tag, tag_no_case, take, take_while_m_n},
	character::complete::{anychar, char, u16, u32},
	combinator::{cut, into, map, map_res, opt, recognize, value as map_value},
	multi::{many0, separated_list1},
	number::complete::recognize_float,
	sequence::{delimited, preceded, terminated, tuple},
	Err,
};

pub fn analyzer(i: &str) -> IResult<&str, DefineAnalyzerStatement> {
	let (i, _) = tag_no_case("ANALYZER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(analyzer_opts)(i)?;
	let (i, _) = expected("one of FILTERS, TOKENIZERS, or COMMENT", ending::query)(i)?;
	// Create the base statement
	let mut res = DefineAnalyzerStatement {
		name,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineAnalyzerOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineAnalyzerOption::Filters(v) => {
				res.filters = Some(v);
			}
			DefineAnalyzerOption::Tokenizers(v) => {
				res.tokenizers = Some(v);
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineAnalyzerOption {
	Comment(Strand),
	Filters(Vec<Filter>),
	Tokenizers(Vec<Tokenizer>),
}

fn analyzer_opts(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	alt((analyzer_comment, analyzer_filters, analyzer_tokenizers))(i)
}

fn analyzer_comment(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(strand)(i)?;
	Ok((i, DefineAnalyzerOption::Comment(v)))
}

fn analyzer_filters(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FILTERS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(filters)(i)?;
	Ok((i, DefineAnalyzerOption::Filters(v)))
}

fn analyzer_tokenizers(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TOKENIZERS")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = cut(tokenizers)(i)?;
	Ok((i, DefineAnalyzerOption::Tokenizers(v)))
}
