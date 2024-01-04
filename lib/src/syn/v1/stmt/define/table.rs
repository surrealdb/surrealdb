use super::super::super::{
	comment::shouldbespace,
	ending,
	error::expected,
	literal::{ident, strand},
	part::{changefeed, permission::permissions, view},
	IResult,
};
use crate::{
	sql::{
		statements::DefineTableStatement, ChangeFeed, Kind, Permission, Permissions, Strand, View,
	},
	syn::v1::common::verbar,
};
use nom::{
	branch::alt, bytes::complete::tag_no_case, combinator::cut, multi::many0,
	multi::separated_list1,
};

pub fn table(i: &str) -> IResult<&str, DefineTableStatement> {
	let (i, _) = tag_no_case("TABLE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(table_opts)(i)?;
	let (i, _) = expected(
		"RELATION, DROP, SCHEMALESS, SCHEMAFUL(L), VIEW, CHANGEFEED, PERMISSIONS, or COMMENT",
		ending::query,
	)(i)?;
	// Create the base statement
	let mut res = DefineTableStatement {
		name,
		permissions: Permissions::none(),
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
			DefineTableOption::Relation(r) => {
				res.relation = Some((r.from, r.to));
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
	Relation(Relation),
}

#[derive(Debug, Default)]
struct Relation {
	from: Option<Kind>,
	to: Option<Kind>,
}

enum RelationDir {
	From(Kind),
	To(Kind),
}

impl Relation {
	fn merge(&mut self, other: RelationDir) {
		//TODO: error if both self and other are some
		match other {
			RelationDir::From(i) => self.from = Some(i),
			RelationDir::To(i) => self.to = Some(i),
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

fn table_relation(i: &str) -> IResult<&str, DefineTableOption> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("RELATION")(i)?;

	let (i, dirs) = many0(alt((relation_from, relation_to)))(i)?;

	let mut relation: Relation = Default::default();

	for dir in dirs {
		relation.merge(dir);
	}

	Ok((i, DefineTableOption::Relation(relation)))
}

fn relation_from(i: &str) -> IResult<&str, RelationDir> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FROM")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, idents) = separated_list1(verbar, ident)(i)?;
	Ok((i, RelationDir::From(Kind::Record(idents.into_iter().map(Into::into).collect()))))
}

fn relation_to(i: &str) -> IResult<&str, RelationDir> {
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TO")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, idents) = separated_list1(verbar, ident)(i)?;

	Ok((i, RelationDir::To(Kind::Record(idents.into_iter().map(Into::into).collect()))))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn define_table_with_changefeed() {
		let sql = "TABLE mytable SCHEMALESS CHANGEFEED 1h PERMISSIONS NONE";
		let res = table(sql);
		let out = res.unwrap().1;
		assert_eq!(format!("DEFINE {sql}"), format!("{}", out));

		let serialized: Vec<u8> = (&out).try_into().unwrap();
		let deserialized = DefineTableStatement::try_from(&serialized).unwrap();
		assert_eq!(out, deserialized);
	}
}
