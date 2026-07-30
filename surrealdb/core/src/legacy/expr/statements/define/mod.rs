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
pub(crate) mod model;
pub(crate) mod module;
pub(crate) mod namespace;
pub(crate) mod param;
pub(crate) mod sequence;
pub(crate) mod table;
pub(crate) mod user;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::expr::statements::define::DefineStatement;
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineStatement::compute", skip_all)]
pub(crate) async fn define_statement_compute(
	this: &DefineStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	match this {
		DefineStatement::Namespace(v) => {
			crate::legacy::define_namespace_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Database(v) => {
			crate::legacy::define_database_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Function(v) => {
			crate::legacy::define_function_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Param(v) => {
			crate::legacy::define_param_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Table(v) => {
			crate::legacy::define_table_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Event(v) => {
			crate::legacy::define_event_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Field(v) => {
			crate::legacy::define_field_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Index(v) => {
			crate::legacy::define_index_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Analyzer(v) => {
			crate::legacy::define_analyzer_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::User(v) => {
			crate::legacy::define_user_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Model(v) => {
			crate::legacy::define_model_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Access(v) => {
			crate::legacy::define_access_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Config(v) => {
			crate::legacy::define_config_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Api(v) => {
			crate::legacy::define_api_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Bucket(v) => {
			crate::legacy::define_bucket_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Sequence(v) => {
			crate::legacy::define_sequence_statement_compute(v, stk, ctx, opt, doc).await
		}
		DefineStatement::Module(v) => {
			crate::legacy::define_module_statement_compute(v, stk, ctx, opt, doc).await
		}
	}
}

/// Validate a user-supplied `GRAPHQL_ALIAS "..."` value against the GraphQL
/// `Name` production (`/[_A-Za-z][_0-9A-Za-z]*/`).
///
/// Returning a clear definition-time error here is preferable to silently
/// falling back at schema-generation time — a typo like
/// `GRAPHQL_ALIAS "first name"` would otherwise produce a working schema with
/// the un-aliased name and no feedback to the user.
pub(crate) fn validate_graphql_alias(alias: &Option<String>, kind: &str) -> Result<()> {
	let Some(alias) = alias.as_deref() else {
		return Ok(());
	};
	let mut chars = alias.chars();
	let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
	let rest_ok = chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
	if !first_ok || !rest_ok {
		return Err(ExecError::InvalidStatement(format!(
			"GRAPHQL_ALIAS `{alias}` on {kind} is not a valid GraphQL Name; \
			 expected /[_A-Za-z][_0-9A-Za-z]*/"
		))
		.into());
	}
	Ok(())
}
