use super::{comment::mightbespace, common::colons, value::value, IResult};
use crate::sql::{Statement, Statements};
use nom::{
	branch::alt,
	combinator::map,
	multi::{many0, separated_list1},
	sequence::delimited,
};

mod analyze;
mod begin;
mod cancel;
mod commit;
mod create;
mod define;
mod delete;
mod flow;
mod foreach;
mod ifelse;
mod info;
mod insert;
mod kill;
mod live;
mod option;
mod output;
mod rebuild;
mod relate;
mod remove;
mod select;
mod set;
mod show;
mod sleep;
mod throw;
mod update;
mod r#use;

pub use analyze::analyze;
pub use begin::begin;
pub use cancel::cancel;
pub use commit::commit;
pub use create::create;
pub use define::define;
pub use delete::delete;
pub use flow::{r#break, r#continue};
pub use foreach::foreach;
pub use ifelse::ifelse;
pub use info::info;
pub use insert::insert;
pub use kill::kill;
pub use live::live;
pub use option::option;
pub use output::output;
pub use r#use::r#use;
pub use rebuild::rebuild;
pub use relate::relate;
pub use remove::remove;
pub use select::select;
pub use set::set;
pub use show::show;
pub use sleep::sleep;
pub use throw::throw;
pub use update::update;

pub fn statements(i: &str) -> IResult<&str, Statements> {
	let (i, v) = separated_list1(colons, statement)(i)?;
	let (i, _) = many0(colons)(i)?;
	Ok((i, Statements(v)))
}

pub fn statement(i: &str) -> IResult<&str, Statement> {
	delimited(
		mightbespace,
		alt((
			alt((
				map(analyze, Statement::Analyze),
				map(begin, Statement::Begin),
				map(r#break, Statement::Break),
				map(cancel, Statement::Cancel),
				map(commit, Statement::Commit),
				map(r#continue, Statement::Continue),
				map(create, Statement::Create),
				map(define, Statement::Define),
				map(delete, Statement::Delete),
				map(foreach, Statement::Foreach),
				map(ifelse, Statement::Ifelse),
				map(info, Statement::Info),
				map(insert, Statement::Insert),
			)),
			alt((
				map(kill, Statement::Kill),
				map(live, Statement::Live),
				map(option, Statement::Option),
				map(output, Statement::Output),
				map(rebuild, Statement::Rebuild),
				map(relate, Statement::Relate),
				map(remove, Statement::Remove),
				map(select, Statement::Select),
				map(set, Statement::Set),
				map(show, Statement::Show),
				map(sleep, Statement::Sleep),
				map(throw, Statement::Throw),
				map(update, Statement::Update),
				map(r#use, Statement::Use),
			)),
			map(value, Statement::Value),
		)),
		mightbespace,
	)(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn single_statement() {
		let sql = "CREATE test";
		let res = statement(sql);
		let out = res.unwrap().1;
		assert_eq!("CREATE test", format!("{}", out))
	}

	#[test]
	fn multiple_statements() {
		let sql = "CREATE test; CREATE temp;";
		let res = statements(sql);
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}

	#[test]
	fn multiple_statements_semicolons() {
		let sql = "CREATE test;;;CREATE temp;;;";
		let res = statements(sql);
		let out = res.unwrap().1;
		assert_eq!("CREATE test;\nCREATE temp;", format!("{}", out))
	}

	#[test]
	fn show_table_changes() {
		let sql = "SHOW CHANGES FOR TABLE test SINCE 123456";
		let res = statement(sql);
		let out = res.unwrap().1;
		assert_eq!("SHOW CHANGES FOR TABLE test SINCE 123456", format!("{}", out))
	}

	#[test]
	fn show_database_changes() {
		let sql = "SHOW CHANGES FOR DATABASE SINCE 123456";
		let res = statement(sql);
		let out = res.unwrap().1;
		assert_eq!("SHOW CHANGES FOR DATABASE SINCE 123456", format!("{}", out))
	}
}
