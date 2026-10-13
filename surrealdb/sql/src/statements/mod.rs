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

#[cfg(test)]
mod tests {
	use std::default::Default;

	use surrealdb_types::ToSql;

	use super::*;

	// Ensure that both Default & ToSQL exist
	macro_rules! gentest_default_tosql {
        ($($t:ty, $name:ident),* $(,)?) => {$(
            #[test]
            fn $name(){
                <$t>::default().to_sql();
            }
        )*};
    }

	gentest_default_tosql!(AlterAccessStatement, test_alter_access_statement);
	gentest_default_tosql!(AlterAnalyzerStatement, test_alter_analyzer_statement);
	gentest_default_tosql!(AlterApiStatement, test_alter_api_statement);
	gentest_default_tosql!(AlterBucketStatement, test_alter_bucket_statement);
	gentest_default_tosql!(AlterConfigStatement, test_alter_config_statement);
	gentest_default_tosql!(AlterDatabaseStatement, test_alter_database_statement);
	gentest_default_tosql!(AlterEventStatement, test_alter_event_statement);
	gentest_default_tosql!(AlterFieldStatement, test_alter_field_statement);
	gentest_default_tosql!(AlterFunctionStatement, test_alter_function_statement);
	gentest_default_tosql!(AlterIndexStatement, test_alter_index_statement);
	gentest_default_tosql!(AlterModuleStatement, test_alter_module_statement);
	gentest_default_tosql!(AlterNamespaceStatement, test_alter_namespace_statement);
	gentest_default_tosql!(AlterParamStatement, test_alter_param_statement);
	gentest_default_tosql!(AlterSequenceStatement, test_alter_sequence_statement);
	gentest_default_tosql!(AlterSystemStatement, test_alter_system_statement);
	gentest_default_tosql!(AlterTableStatement, test_alter_table_statement);
	gentest_default_tosql!(AlterUserStatement, test_alter_user_statement);
	gentest_default_tosql!(CreateStatement, test_create_statement);
	gentest_default_tosql!(DefineAccessStatement, test_define_access_statement);
	gentest_default_tosql!(DefineAnalyzerStatement, test_define_analyzer_statement);
	gentest_default_tosql!(DefineApiStatement, test_define_api_statement);
	gentest_default_tosql!(DefineBucketStatement, test_define_bucket_statement);
	gentest_default_tosql!(DefineConfigStatement, test_define_config_statement);
	gentest_default_tosql!(DefineDatabaseStatement, test_define_database_statement);
	gentest_default_tosql!(DefineEventStatement, test_define_event_statement);
	gentest_default_tosql!(DefineFieldStatement, test_define_field_statement);
	gentest_default_tosql!(DefineFunctionStatement, test_define_function_statement);
	gentest_default_tosql!(DefineIndexStatement, test_define_index_statement);
	gentest_default_tosql!(DefineModelStatement, test_define_model_statement);
	// Probably the one case that doesn't really make sense
	// gentest_default_tosql!(DefineModuleStatement, test_define_module_statement);
	gentest_default_tosql!(DefineNamespaceStatement, test_define_namespace_statement);
	gentest_default_tosql!(DefineParamStatement, test_define_param_statement);
	gentest_default_tosql!(DefineSequenceStatement, test_define_sequence_statement);
	gentest_default_tosql!(DefineTableStatement, test_define_table_statement);
	gentest_default_tosql!(DefineUserStatement, test_define_user_statement);
	gentest_default_tosql!(DeleteStatement, test_delete_statement);
	gentest_default_tosql!(ForeachStatement, test_foreach_statement);
	gentest_default_tosql!(IfelseStatement, test_ifelse_statement);
	gentest_default_tosql!(InsertStatement, test_insert_statement);
	gentest_default_tosql!(KillStatement, test_kill_statement);
	gentest_default_tosql!(LiveStatement, test_live_statement);
	gentest_default_tosql!(OptionStatement, test_option_statement);
	gentest_default_tosql!(OutputStatement, test_output_statement);
	gentest_default_tosql!(RebuildIndexStatement, test_rebuild_index_statement);
	gentest_default_tosql!(RelateStatement, test_relate_statement);
	gentest_default_tosql!(RemoveAccessStatement, test_remove_access_statement);
	gentest_default_tosql!(RemoveAnalyzerStatement, test_remove_analyzer_statement);
	gentest_default_tosql!(RemoveApiStatement, test_remove_api_statement);
	gentest_default_tosql!(RemoveBucketStatement, test_remove_bucket_statement);
	gentest_default_tosql!(RemoveConfigStatement, test_remove_config_statement);
	gentest_default_tosql!(RemoveDatabaseStatement, test_remove_database_statement);
	gentest_default_tosql!(RemoveEventStatement, test_remove_event_statement);
	gentest_default_tosql!(RemoveFieldStatement, test_remove_field_statement);
	gentest_default_tosql!(RemoveFunctionStatement, test_remove_function_statement);
	gentest_default_tosql!(RemoveIndexStatement, test_remove_index_statement);
	gentest_default_tosql!(RemoveModelStatement, test_remove_model_statement);
	gentest_default_tosql!(RemoveModuleStatement, test_remove_module_statement);
	gentest_default_tosql!(RemoveNamespaceStatement, test_remove_namespace_statement);
	gentest_default_tosql!(RemoveParamStatement, test_remove_param_statement);
	gentest_default_tosql!(RemoveSequenceStatement, test_remove_sequence_statement);
	gentest_default_tosql!(RemoveTableStatement, test_remove_table_statement);
	gentest_default_tosql!(RemoveUserStatement, test_remove_user_statement);
	gentest_default_tosql!(SelectStatement, test_select_statement);
	gentest_default_tosql!(SetStatement, test_set_statement);
	gentest_default_tosql!(ShowStatement, test_show_statement);
	gentest_default_tosql!(SleepStatement, test_sleep_statement);
	gentest_default_tosql!(UpdateStatement, test_update_statement);
	gentest_default_tosql!(UpsertStatement, test_upsert_statement);
}
