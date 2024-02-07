use super::super::super::{
	comment::shouldbespace,
	ending,
	error::expected,
	literal::{filters, ident, strand, tokenizer::tokenizers},
	IResult,
};
#[cfg(feature = "sql2")]
use crate::sql::Ident;
use crate::sql::{filter::Filter, statements::DefineAnalyzerStatement, Strand, Tokenizer};
#[cfg(feature = "sql2")]
use nom::bytes::complete::tag;
use nom::{branch::alt, bytes::complete::tag_no_case, combinator::cut, multi::many0};

pub fn analyzer(i: &str) -> IResult<&str, DefineAnalyzerStatement> {
	let (i, _) = tag_no_case("ANALYZER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(analyzer_opts)(i)?;
	let (i, _) = expected("one of FUNCTION, FILTERS, TOKENIZERS, or COMMENT", ending::query)(i)?;
	// Create the base statement
	let mut res = DefineAnalyzerStatement {
		name,
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			#[cfg(feature = "sql2")]
			DefineAnalyzerOption::Function(i) => {
				res.function = Some(i);
			}
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
	#[cfg(feature = "sql2")]
	Function(Ident),
	Comment(Strand),
	Filters(Vec<Filter>),
	Tokenizers(Vec<Tokenizer>),
}

fn analyzer_opts(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	alt((
		#[cfg(feature = "sql2")]
		analyzer_function,
		analyzer_comment,
		analyzer_filters,
		analyzer_tokenizers,
	))(i)
}

#[cfg(feature = "sql2")]
fn analyzer_function(i: &str) -> IResult<&str, DefineAnalyzerOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FUNCTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag("fn::")(i)?;
	let (i, name) = ident(i)?;
	Ok((i, DefineAnalyzerOption::Function(name)))
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
