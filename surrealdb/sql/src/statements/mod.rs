pub mod access;
pub mod alter;
pub mod create;
pub mod define;
pub mod delete;
pub mod foreach;
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
pub mod update;
pub mod upsert;
pub mod r#use;

pub use self::access::AccessStatement;
pub use self::alter::{AlterStatement, AlterTableStatement};
pub use self::create::CreateStatement;
pub use self::define::{
	DefineApiStatement, DefineFunctionStatement, DefineIndexStatement, DefineModelStatement,
	DefineModuleStatement, DefineNamespaceStatement, DefineStatement, DefineTableStatement,
};
pub use self::delete::DeleteStatement;
pub use self::foreach::ForeachStatement;
pub use self::ifelse::IfelseStatement;
pub use self::info::InfoStatement;
pub use self::insert::InsertStatement;
pub use self::kill::KillStatement;
pub use self::live::LiveStatement;
pub use self::option::OptionStatement;
pub use self::output::OutputStatement;
pub use self::rebuild::RebuildStatement;
pub use self::relate::RelateStatement;
pub use self::remove::{
	RemoveAccessStatement, RemoveDatabaseStatement, RemoveEventStatement, RemoveFieldStatement,
	RemoveFunctionStatement, RemoveIndexStatement, RemoveNamespaceStatement, RemoveParamStatement,
	RemoveStatement, RemoveTableStatement, RemoveUserStatement,
};
pub use self::select::SelectStatement;
pub use self::set::SetStatement;
pub use self::show::ShowStatement;
pub use self::sleep::SleepStatement;
pub use self::update::UpdateStatement;
pub use self::upsert::UpsertStatement;
pub use self::r#use::UseStatement;
use crate::statements::alter::{
	AlterAccessStatement, AlterAnalyzerStatement, AlterApiStatement, AlterBucketStatement,
	AlterConfigStatement, AlterDatabaseStatement, AlterEventStatement, AlterFieldStatement,
	AlterFunctionStatement, AlterIndexStatement, AlterModuleStatement, AlterNamespaceStatement,
	AlterParamStatement, AlterSequenceStatement, AlterSystemStatement, AlterUserStatement,
};
use crate::statements::define::{
	DefineAccessStatement, DefineAnalyzerStatement, DefineBucketStatement, DefineConfigStatement,
	DefineDatabaseStatement, DefineEventStatement, DefineFieldStatement, DefineParamStatement,
	DefineSequenceStatement, DefineUserStatement,
};
use crate::statements::rebuild::RebuildIndexStatement;
use crate::statements::remove::{
	RemoveAnalyzerStatement, RemoveApiStatement, RemoveBucketStatement, RemoveConfigStatement,
	RemoveModelStatement, RemoveModuleStatement, RemoveSequenceStatement,
};

/// Allow statements to be fed into `query(stmt)`...
macro_rules! impl_into_cow {
    ($($t:ty),* $(,)?) => {$(
        impl<'r> From<$t> for std::borrow::Cow<'r, str> {
            fn from(v: $t) -> Self { std::borrow::Cow::Owned(surrealdb_types::ToSql::to_sql(&v)) }
        }
    )*};
}
impl_into_cow!(AlterAccessStatement);
impl_into_cow!(AlterAnalyzerStatement);
impl_into_cow!(AlterApiStatement);
impl_into_cow!(AlterBucketStatement);
impl_into_cow!(AlterConfigStatement);
impl_into_cow!(AlterDatabaseStatement);
impl_into_cow!(AlterEventStatement);
impl_into_cow!(AlterFieldStatement);
impl_into_cow!(AlterFunctionStatement);
impl_into_cow!(AlterIndexStatement);
impl_into_cow!(AlterModuleStatement);
impl_into_cow!(AlterNamespaceStatement);
impl_into_cow!(AlterParamStatement);
impl_into_cow!(AlterSequenceStatement);
impl_into_cow!(AlterSystemStatement);
impl_into_cow!(AlterTableStatement);
impl_into_cow!(AlterUserStatement);
impl_into_cow!(CreateStatement);
impl_into_cow!(DefineAccessStatement);
impl_into_cow!(DefineAnalyzerStatement);
impl_into_cow!(DefineApiStatement);
impl_into_cow!(DefineBucketStatement);
impl_into_cow!(DefineConfigStatement);
impl_into_cow!(DefineDatabaseStatement);
impl_into_cow!(DefineEventStatement);
impl_into_cow!(DefineFieldStatement);
impl_into_cow!(DefineFunctionStatement);
impl_into_cow!(DefineIndexStatement);
impl_into_cow!(DefineModelStatement);
impl_into_cow!(DefineModuleStatement);
impl_into_cow!(DefineNamespaceStatement);
impl_into_cow!(DefineParamStatement);
impl_into_cow!(DefineSequenceStatement);
impl_into_cow!(DefineTableStatement);
impl_into_cow!(DefineUserStatement);
impl_into_cow!(DeleteStatement);
impl_into_cow!(ForeachStatement);
impl_into_cow!(IfelseStatement);
impl_into_cow!(InsertStatement);
impl_into_cow!(KillStatement);
impl_into_cow!(LiveStatement);
impl_into_cow!(OptionStatement);
impl_into_cow!(OutputStatement);
impl_into_cow!(RebuildIndexStatement);
impl_into_cow!(RelateStatement);
impl_into_cow!(RemoveAccessStatement);
impl_into_cow!(RemoveAnalyzerStatement);
impl_into_cow!(RemoveApiStatement);
impl_into_cow!(RemoveBucketStatement);
impl_into_cow!(RemoveConfigStatement);
impl_into_cow!(RemoveDatabaseStatement);
impl_into_cow!(RemoveEventStatement);
impl_into_cow!(RemoveFieldStatement);
impl_into_cow!(RemoveFunctionStatement);
impl_into_cow!(RemoveIndexStatement);
impl_into_cow!(RemoveModelStatement);
impl_into_cow!(RemoveModuleStatement);
impl_into_cow!(RemoveNamespaceStatement);
impl_into_cow!(RemoveParamStatement);
impl_into_cow!(RemoveSequenceStatement);
impl_into_cow!(RemoveTableStatement);
impl_into_cow!(RemoveUserStatement);
impl_into_cow!(SelectStatement);
impl_into_cow!(SetStatement);
impl_into_cow!(ShowStatement);
impl_into_cow!(SleepStatement);
impl_into_cow!(UpdateStatement);
impl_into_cow!(UpsertStatement);
