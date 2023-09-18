use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::sql::base::Base;
use crate::sql::changefeed::{changefeed, ChangeFeed};
use crate::sql::comment::shouldbespace;
use crate::sql::ending;
use crate::sql::error::expected;
use crate::sql::error::IResult;
use crate::sql::fmt::is_pretty;
use crate::sql::fmt::pretty_indent;
use crate::sql::ident::{ident, Ident};
use crate::sql::permission::{permissions, Permissions};
use crate::sql::statements::UpdateStatement;
use crate::sql::strand::{strand, Strand};
use crate::sql::value::{Value, Values};
use crate::sql::view::{view, View};
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use nom::multi::many0;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineTableStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<Strand>,
}

impl DefineTableStatement {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Process the statement
		let key = crate::key::database::tb::new(opt.ns(), opt.db(), &self.name);
		let ns = run.add_ns(opt.ns(), opt.strict).await?;
		let db = run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		let dt = if self.id.is_none() && ns.id.is_some() && db.id.is_some() {
			let mut tb = self.clone();
			tb.id = Some(run.get_next_tb_id(ns.id.unwrap(), db.id.unwrap()).await?);
			run.set(key, &tb).await?;
			tb
		} else {
			run.set(key, self).await?;
			self.to_owned()
		};
		// Check if table is a view
		if let Some(view) = &self.view {
			// Remove the table data
			let key = crate::key::table::all::new(opt.ns(), opt.db(), &self.name);
			run.delp(key, u32::MAX).await?;
			// Process each foreign table
			for v in view.what.0.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(opt.ns(), opt.db(), v, &self.name);
				run.set(key, self).await?;
				// Clear the cache
				let key = crate::key::table::ft::prefix(opt.ns(), opt.db(), v);
				run.clr(key).await?;
			}
			// Release the transaction
			drop(run);
			// Force queries to run
			let opt = &opt.new_with_force(true);
			// Don't process field queries
			let opt = &opt.new_with_fields(false);
			// Don't process event queries
			let opt = &opt.new_with_events(false);
			// Don't process index queries
			let opt = &opt.new_with_indexes(false);
			// Process each foreign table
			for v in view.what.0.iter() {
				// Process the view data
				let stm = UpdateStatement {
					what: Values(vec![Value::Table(v.clone())]),
					..UpdateStatement::default()
				};
				stm.compute(ctx, opt, txn, doc).await?;
			}
		} else if dt.changefeed.is_some() {
			run.record_table_change(opt.ns(), opt.db(), self.name.0.as_str(), &dt);
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TABLE {}", self.name)?;
		if self.drop {
			f.write_str(" DROP")?;
		}
		f.write_str(if self.full {
			" SCHEMAFULL"
		} else {
			" SCHEMALESS"
		})?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		if let Some(ref v) = self.view {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		if !self.permissions.is_full() {
			let _indent = if is_pretty() {
				Some(pretty_indent())
			} else {
				f.write_char(' ')?;
				None
			};
			write!(f, "{}", self.permissions)?;
		}
		Ok(())
	}
}

pub fn table(i: &str) -> IResult<&str, DefineTableStatement> {
	let (i, _) = tag_no_case("TABLE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, opts) = many0(table_opts)(i)?;
	let (i, _) = expected(
		"DROP, SCHEMALESS, SCHEMAFUL(L), VIEW, CHANGEFEED, PERMISSIONS, or COMMENT",
		ending::query,
	)(i)?;
	// Create the base statement
	let mut res = DefineTableStatement {
		name,
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
		}
	}
	// Return the statement
	Ok((i, res))
}

enum DefineTableOption {
	Drop,
	View(View),
	Schemaless,
	Schemafull,
	Comment(Strand),
	Permissions(Permissions),
	ChangeFeed(ChangeFeed),
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
	let (i, v) = permissions(i)?;
	Ok((i, DefineTableOption::Permissions(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn define_table_with_changefeed() {
		let sql = "TABLE mytable SCHEMALESS CHANGEFEED 1h";
		let res = table(sql);
		let out = res.unwrap().1;
		assert_eq!(format!("DEFINE {sql}"), format!("{}", out));

		let serialized: Vec<u8> = (&out).try_into().unwrap();
		let deserialized = DefineTableStatement::try_from(&serialized).unwrap();
		assert_eq!(out, deserialized);
	}
}
