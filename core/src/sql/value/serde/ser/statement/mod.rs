pub mod analyze;
pub mod begin;
pub mod r#break;
pub mod cancel;
pub mod commit;
pub mod r#continue;
pub mod create;
pub mod define;
pub mod delete;
pub mod ifelse;
pub mod info;
pub mod insert;
pub mod kill;
pub mod live;
pub mod option;
pub mod output;
pub mod rebuild;
pub mod relate;
pub mod remove;
pub mod select;
pub mod set;
pub mod show;
pub mod sleep;
pub mod throw;
pub mod update;
pub mod upsert;
pub mod vec;
pub mod yuse;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Statement;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Statement;
	type Error = Error;

	type SerializeSeq = Impossible<Statement, Error>;
	type SerializeTuple = Impossible<Statement, Error>;
	type SerializeTupleStruct = Impossible<Statement, Error>;
	type SerializeTupleVariant = Impossible<Statement, Error>;
	type SerializeMap = Impossible<Statement, Error>;
	type SerializeStruct = Impossible<Statement, Error>;
	type SerializeStructVariant = Impossible<Statement, Error>;

	const EXPECTED: &'static str = "an enum `Statement`";

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Analyze" => Ok(Statement::Analyze(value.serialize(analyze::Serializer.wrap())?)),
			"Begin" => Ok(Statement::Begin(value.serialize(begin::Serializer.wrap())?)),
			"Break" => Ok(Statement::Break(value.serialize(r#break::Serializer.wrap())?)),
			"Cancel" => Ok(Statement::Cancel(value.serialize(cancel::Serializer.wrap())?)),
			"Commit" => Ok(Statement::Commit(value.serialize(commit::Serializer.wrap())?)),
			"Continue" => Ok(Statement::Continue(value.serialize(r#continue::Serializer.wrap())?)),
			"Create" => Ok(Statement::Create(value.serialize(create::Serializer.wrap())?)),
			"Define" => Ok(Statement::Define(value.serialize(define::Serializer.wrap())?)),
			"Delete" => Ok(Statement::Delete(value.serialize(delete::Serializer.wrap())?)),
			"Ifelse" => Ok(Statement::Ifelse(value.serialize(ifelse::Serializer.wrap())?)),
			"Info" => Ok(Statement::Info(value.serialize(info::Serializer.wrap())?)),
			"Insert" => Ok(Statement::Insert(value.serialize(insert::Serializer.wrap())?)),
			"Kill" => Ok(Statement::Kill(value.serialize(kill::Serializer.wrap())?)),
			"Live" => Ok(Statement::Live(value.serialize(live::Serializer.wrap())?)),
			"Option" => Ok(Statement::Option(value.serialize(option::Serializer.wrap())?)),
			"Output" => Ok(Statement::Output(value.serialize(output::Serializer.wrap())?)),
			"Rebuild" => Ok(Statement::Rebuild(value.serialize(rebuild::Serializer.wrap())?)),
			"Relate" => Ok(Statement::Relate(value.serialize(relate::Serializer.wrap())?)),
			"Remove" => Ok(Statement::Remove(value.serialize(remove::Serializer.wrap())?)),
			"Select" => Ok(Statement::Select(value.serialize(select::Serializer.wrap())?)),
			"Set" => Ok(Statement::Set(value.serialize(set::Serializer.wrap())?)),
			"Show" => Ok(Statement::Show(value.serialize(show::Serializer.wrap())?)),
			"Sleep" => Ok(Statement::Sleep(value.serialize(sleep::Serializer.wrap())?)),
			"Throw" => Ok(Statement::Throw(value.serialize(throw::Serializer.wrap())?)),
			"Update" => Ok(Statement::Update(value.serialize(update::Serializer.wrap())?)),
			"Upsert" => Ok(Statement::Upsert(value.serialize(upsert::Serializer.wrap())?)),
			"Use" => Ok(Statement::Use(value.serialize(yuse::Serializer.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::statements::analyze::AnalyzeStatement;
	use crate::sql::statements::DefineStatement;
	use crate::sql::statements::InfoStatement;
	use crate::sql::statements::RemoveStatement;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn analyze() {
		let statement =
			Statement::Analyze(AnalyzeStatement::Idx(Default::default(), Default::default()));
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn begin() {
		let statement = Statement::Begin(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn cancel() {
		let statement = Statement::Cancel(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn commit() {
		let statement = Statement::Commit(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn create() {
		let statement = Statement::Create(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn define() {
		let statement = Statement::Define(DefineStatement::Namespace(Default::default()));
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn delete() {
		let statement = Statement::Delete(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn ifelse() {
		let statement = Statement::Ifelse(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn info() {
		let statement = Statement::Info(InfoStatement::Ns(Default::default()));
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn insert() {
		let statement = Statement::Insert(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn kill() {
		let statement = Statement::Kill(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn live() {
		let statement = Statement::Live(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn option() {
		let statement = Statement::Option(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn output() {
		let statement = Statement::Output(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn relate() {
		let statement = Statement::Relate(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn remove() {
		let statement = Statement::Remove(RemoveStatement::Table(Default::default()));
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn select() {
		let statement = Statement::Select(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn set() {
		let statement = Statement::Set(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn show() {
		let statement = Statement::Show(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn sleep() {
		let statement = Statement::Sleep(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn update() {
		let statement = Statement::Update(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn upsert() {
		let statement = Statement::Upsert(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}

	#[test]
	fn yuse() {
		let statement = Statement::Use(Default::default());
		let serialized = statement.serialize(Serializer.wrap()).unwrap();
		assert_eq!(statement, serialized);
	}
}
