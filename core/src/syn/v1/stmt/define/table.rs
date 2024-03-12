use super::super::super::{
	comment::shouldbespace,
	ending,
	error::expected,
	literal::{ident, strand},
	part::{changefeed, permission::permissions, view},
	IResult,
};
use crate::sql::{
	statements::DefineTableStatement, ChangeFeed, Permission, Permissions, Strand, View,
};
#[cfg(feature = "sql2")]
use crate::{
	sql::{Kind, Relation, TableType},
	syn::v1::common::verbar,
	syn::v1::ParseError,
};

use nom::{branch::alt, bytes::complete::tag_no_case, combinator::cut, multi::many0};
#[cfg(feature = "sql2")]
use nom::{multi::separated_list1, Err, combinator::opt, sequence::tuple};

pub fn table(i: &str) -> IResult<&str, DefineTableStatement> {
	let (i, _) = tag_no_case("TABLE")(i)?;
	#[cfg(feature = "sql2")]
	let (i, if_not_exists) = opt(tuple((
		shouldbespace,
		tag_no_case("IF"),
		cut(tuple((shouldbespace, tag_no_case("NOT"), shouldbespace, tag_no_case("EXISTS")))),
	)))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(table_opts)(i)?;
	let (i, _) = expected(
		"TYPE, RELATION, DROP, SCHEMALESS, SCHEMAFUL(L), VIEW, CHANGEFEED, PERMISSIONS, or COMMENT",
		ending::query,
	)(i)?;
	// Create the base statement
	let mut res = DefineTableStatement {
		name,
		permissions: Permissions::none(),
		// Default to ANY if not specified in the DEFINE statement
		#[cfg(feature = "sql2")]
		table_type: TableType::Any,
		#[cfg(feature = "sql2")]
		if_not_exists: if_not_exists.is_some(),
		..Default::default()
	};
	// Assign any defined options
	for opt in opts {
		match opt {
			DefineTableOption::Drop => {
				res.drop = true;
			}
			DefineTableOption::Schemafull => {
				res.full = true;
			}
			DefineTableOption::Schemaless => {
				res.full = false;
			}
			DefineTableOption::View(v) => {
				res.view = Some(v);
			}
			DefineTableOption::Comment(v) => {
				res.comment = Some(v);
			}
			DefineTableOption::ChangeFeed(v) => {
				res.changefeed = Some(v);
			}
			DefineTableOption::Permissions(v) => {
				res.permissions = v;
			}
			#[cfg(feature = "sql2")]
			DefineTableOption::TableType(t) => {
				res.table_type = t;
			}
		}
	}
	// Return the statement
	Ok((i, res))
}

#[derive(Debug)]
enum DefineTableOption {
	Drop,
	View(View),
	Schemaless,
	Schemafull,
	Comment(Strand),
	Permissions(Permissions),
	ChangeFeed(ChangeFeed),
	#[cfg(feature = "sql2")]
	TableType(TableType),
}

#[cfg(feature = "sql2")]
enum RelationDir {
	From(Kind),
	To(Kind),
}

#[cfg(feature = "sql2")]
impl Relation {
	fn merge<'a>(&mut self, i: &'a str, other: RelationDir) -> IResult<&'a str, ()> {
		//TODO: error if both self and other are some
		match other {
			RelationDir::From(f) => {
				if self.from.is_some() {
					Err(Err::Failure(ParseError::Expected {
						tried: i,
						expected: "only one IN clause",
					}))
				} else {
					self.from = Some(f);
					Ok((i, ()))
				}
			}
			RelationDir::To(t) => {
				if self.to.is_some() {
					Err(Err::Failure(ParseError::Expected {
						tried: i,
						expected: "only one OUT clause",
					}))
				} else {
					self.to = Some(t);
					Ok((i, ()))
				}
			}
		}
	}
}

fn table_opts(i: &str) -> IResult<&str, DefineTableOption> {
	alt((
		table_drop,
		table_view,
		table_comment,
		table_schemaless,
		table_schemafull,
		table_permissions,
		table_changefeed,
		#[cfg(feature = "sql2")]
		table_type,
		#[cfg(feature = "sql2")]
		table_relation,
	))(i)
}

fn table_drop(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("DROP")(i)?;
	Ok((i, DefineTableOption::Drop))
}

fn table_changefeed(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = changefeed(i)?;
	Ok((i, DefineTableOption::ChangeFeed(v)))
}

fn table_view(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = view(i)?;
	Ok((i, DefineTableOption::View(v)))
}

fn table_schemaless(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SCHEMALESS")(i)?;
	Ok((i, DefineTableOption::Schemaless))
}

fn table_schemafull(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("SCHEMAFULL"), tag_no_case("SCHEMAFUL")))(i)?;
	Ok((i, DefineTableOption::Schemafull))
}

fn table_comment(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("COMMENT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = strand(i)?;
	Ok((i, DefineTableOption::Comment(v)))
}

fn table_permissions(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = permissions(i, Permission::None)?;
	Ok((i, DefineTableOption::Permissions(v)))
}

#[cfg(feature = "sql2")]
fn table_type(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TYPE")(i)?;
	alt((table_normal, table_any, table_relation))(i)
}

#[cfg(feature = "sql2")]
fn table_normal(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("NORMAL")(i)?;
	Ok((i, DefineTableOption::TableType(TableType::Normal)))
}

#[cfg(feature = "sql2")]
fn table_any(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ANY")(i)?;
	Ok((i, DefineTableOption::TableType(TableType::Any)))
}

#[cfg(feature = "sql2")]
fn table_relation(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("RELATION")(i)?;

	let (i, dirs) = many0(alt((relation_from, relation_to)))(i)?;

	let mut relation: Relation = Default::default();

	for dir in dirs {
		relation.merge(i, dir)?;
	}

	Ok((i, DefineTableOption::TableType(TableType::Relation(relation))))
}

#[cfg(feature = "sql2")]
fn relation_from(i: &str) -> IResult<&str, RelationDir> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("FROM"), tag_no_case("IN")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, idents) = separated_list1(verbar, ident)(i)?;
	Ok((i, RelationDir::From(Kind::Record(idents.into_iter().map(Into::into).collect()))))
}

#[cfg(feature = "sql2")]
fn relation_to(i: &str) -> IResult<&str, RelationDir> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("TO"), tag_no_case("OUT")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, idents) = separated_list1(verbar, ident)(i)?;

	Ok((i, RelationDir::To(Kind::Record(idents.into_iter().map(Into::into).collect()))))
}

#[cfg(test)]
#[cfg(feature = "sql2")]
mod tests {

	use super::*;

	#[test]
	fn define_table_with_changefeed() {
		let sql = "TABLE mytable TYPE ANY SCHEMALESS CHANGEFEED 1h PERMISSIONS NONE";
		let res = table(sql);
		let out = res.unwrap().1;
		assert_eq!(format!("DEFINE {sql}"), format!("{}", out));

		let serialized: Vec<u8> = (&out).into();
		let deserialized = DefineTableStatement::from(&serialized);
		assert_eq!(out, deserialized);
	}
}
