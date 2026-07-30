pub(crate) mod access;
pub(crate) mod analyzer;
pub(crate) mod api;
pub(crate) mod bucket;
pub(crate) mod config;
pub(crate) mod database;
pub(crate) mod event;
pub(crate) mod field;
pub(crate) mod function;
pub(crate) mod index;
pub(crate) mod module;
pub(crate) mod namespace;
pub(crate) mod param;
pub(crate) mod sequence;
pub(crate) mod system;
pub(crate) mod table;
pub(crate) mod user;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::alter::AlterStatement;
use crate::val::Value;

/// Executes this statement, returning a simple value.
///
/// All `ALTER` statements currently return `Value::None` on success and may
/// perform side effects such as storage compaction or metadata updates.
#[instrument(level = "trace", name = "AlterStatement::compute", skip_all)]
pub(crate) async fn alter_statement_compute(
	this: &AlterStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	match this {
		AlterStatement::System(v) => {
			crate::legacy::alter_system_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Namespace(v) => {
			crate::legacy::alter_namespace_statement_compute(v, ctx, opt).await
		}
		AlterStatement::Database(v) => {
			crate::legacy::alter_database_statement_compute(v, ctx, opt).await
		}
		AlterStatement::Table(v) => {
			crate::legacy::alter_table_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Api(v) => {
			crate::legacy::alter_api_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Event(v) => {
			crate::legacy::alter_event_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Index(v) => {
			crate::legacy::alter_index_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Sequence(v) => {
			crate::legacy::alter_sequence_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Field(v) => {
			crate::legacy::alter_field_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Param(v) => {
			crate::legacy::alter_param_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Bucket(v) => {
			crate::legacy::alter_bucket_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Config(v) => {
			crate::legacy::alter_config_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Analyzer(v) => {
			crate::legacy::alter_analyzer_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Function(v) => {
			crate::legacy::alter_function_statement_compute(v, ctx, opt).await
		}
		AlterStatement::User(v) => {
			crate::legacy::alter_user_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Access(v) => {
			crate::legacy::alter_access_statement_compute(v, stk, ctx, opt, doc).await
		}
		AlterStatement::Module(v) => {
			crate::legacy::alter_module_statement_compute(v, ctx, opt).await
		}
	}
}
