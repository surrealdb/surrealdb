//! The legacy evaluator.
//!
//! Free-function entry points that evaluate expression, statement and value
//! types against the execution environment (`Stk`, `FrozenContext`, `Options`,
//! `CursorDoc`). Functions that evaluate a specific type take that type as
//! their first parameter (named `this`); the module layout mirrors the source
//! layout of the types under `expr/` and `val/`.
//!
//! This module is the complete home of the legacy evaluation logic so that the
//! expression and value layers carry no dependency on the execution
//! environment.

pub(crate) mod analyzer_function;
pub(crate) mod config;
pub(crate) mod expr;
pub(crate) mod knn;
pub(crate) mod value;

pub(crate) use self::expr::block::block_compute;
pub(crate) use self::expr::closure::closure_expr_compute;
pub(crate) use self::expr::expression::{
	expr_compute, expr_compute_binary, expr_compute_postfix, expr_compute_prefix,
};
pub(crate) use self::expr::fetch::{fetch_compute, fetch_compute_expr};
pub(crate) use self::expr::field::{fields_compute, fields_compute_value};
pub(crate) use self::expr::function::{function_call_compute, function_compute};
pub(crate) use self::expr::idiom::recursion::compute_idiom_recursion;
pub(crate) use self::expr::idiom::{idiom_compute, idiom_substitute_indices};
pub(crate) use self::expr::limit::limit_process;
pub(crate) use self::expr::literal::literal_compute;
pub(crate) use self::expr::lookup::lookup_subject_compute;
pub(crate) use self::expr::model::model_compute;
pub(crate) use self::expr::module::{
	module_executable_run, module_executable_signature, silo_executable_run,
	silo_executable_signature, surrealism_executable_run, surrealism_executable_signature,
};
pub(crate) use self::expr::param::param_compute;
pub(crate) use self::expr::parameterize::{
	expr_to_ident, expr_to_idiom, expr_to_optional_ident, exprs_to_fields,
};
pub(crate) use self::expr::part::{
	recurse_instruction_compute, recursion_plan_compute, recursion_plan_compute_inner,
};
pub(crate) use self::expr::record_id::key::record_id_key_lit_compute;
pub(crate) use self::expr::record_id::range::record_id_key_range_lit_compute;
pub(crate) use self::expr::record_id::record_id_lit_compute;
pub(crate) use self::expr::start::start_process;
pub(crate) use self::expr::statements::access::{
	access_statement_compute, create_grant, revoke_grant, subject_compute,
};
pub(crate) use self::expr::statements::alter::access::{
	alter_access_statement_apply, alter_access_statement_compute,
	alter_access_statement_compute_db, alter_access_statement_compute_ns,
	alter_access_statement_compute_root,
};
pub(crate) use self::expr::statements::alter::alter_statement_compute;
pub(crate) use self::expr::statements::alter::analyzer::alter_analyzer_statement_compute;
pub(crate) use self::expr::statements::alter::api::alter_api_statement_compute;
pub(crate) use self::expr::statements::alter::bucket::alter_bucket_statement_compute;
pub(crate) use self::expr::statements::alter::config::alter_config_statement_compute;
pub(crate) use self::expr::statements::alter::database::alter_database_statement_compute;
pub(crate) use self::expr::statements::alter::event::alter_event_statement_compute;
pub(crate) use self::expr::statements::alter::field::alter_field_statement_compute;
pub(crate) use self::expr::statements::alter::function::alter_function_statement_compute;
pub(crate) use self::expr::statements::alter::index::alter_index_statement_compute;
pub(crate) use self::expr::statements::alter::module::alter_module_statement_compute;
pub(crate) use self::expr::statements::alter::namespace::alter_namespace_statement_compute;
pub(crate) use self::expr::statements::alter::param::alter_param_statement_compute;
pub(crate) use self::expr::statements::alter::sequence::alter_sequence_statement_compute;
pub(crate) use self::expr::statements::alter::system::alter_system_statement_compute;
pub(crate) use self::expr::statements::alter::table::alter_table_statement_compute;
pub(crate) use self::expr::statements::alter::user::{
	alter_user_statement_apply, alter_user_statement_compute, alter_user_statement_compute_db,
	alter_user_statement_compute_ns, alter_user_statement_compute_root,
};
pub(crate) use self::expr::statements::create::create_statement_compute;
pub(crate) use self::expr::statements::define::access::{
	define_access_statement_compute, define_access_statement_reject_es512,
	define_access_statement_to_definition, define_access_statement_uses_es512,
};
pub(crate) use self::expr::statements::define::analyzer::{
	define_analyzer_statement_compute, define_analyzer_statement_to_definition,
};
pub(crate) use self::expr::statements::define::api::define_api_statement_compute;
pub(crate) use self::expr::statements::define::bucket::define_bucket_statement_compute;
pub(crate) use self::expr::statements::define::config::api::api_config_compute;
pub(crate) use self::expr::statements::define::config::defaults::default_config_compute;
pub(crate) use self::expr::statements::define::config::{
	config_inner_compute, define_config_statement_compute,
};
pub(crate) use self::expr::statements::define::database::define_database_statement_compute;
pub(crate) use self::expr::statements::define::define_statement_compute;
pub(crate) use self::expr::statements::define::event::define_event_statement_compute;
pub(crate) use self::expr::statements::define::field::{
	define_field_statement_compute, define_field_statement_disallow_mismatched_types,
	define_field_statement_process_recursive_definitions, define_field_statement_to_definition,
	define_field_statement_validate_computed_cycles,
	define_field_statement_validate_computed_options,
	define_field_statement_validate_flexible_restrictions,
	define_field_statement_validate_reference_options,
};
pub(crate) use self::expr::statements::define::function::define_function_statement_compute;
pub(crate) use self::expr::statements::define::index::{
	define_index_statement_compute, refresh_table_index_cache, run_indexing,
};
pub(crate) use self::expr::statements::define::model::define_model_statement_compute;
pub(crate) use self::expr::statements::define::module::define_module_statement_compute;
pub(crate) use self::expr::statements::define::namespace::define_namespace_statement_compute;
pub(crate) use self::expr::statements::define::param::define_param_statement_compute;
pub(crate) use self::expr::statements::define::sequence::define_sequence_statement_compute;
pub(crate) use self::expr::statements::define::table::{
	define_table_statement_add_in_out_fields, define_table_statement_compute,
	define_table_statement_initialize_aggregate_view,
	define_table_statement_initialize_materialized_view, define_table_statement_initialize_view,
};
pub(crate) use self::expr::statements::define::user::{
	define_user_statement_compute, define_user_statement_to_definition,
};
pub(crate) use self::expr::statements::delete::delete_statement_compute;
pub(crate) use self::expr::statements::foreach::foreach_statement_compute;
pub(crate) use self::expr::statements::ifelse::ifelse_statement_compute;
pub(crate) use self::expr::statements::info::{info_statement_compute, process_modules};
pub(crate) use self::expr::statements::insert::insert_statement_compute;
pub(crate) use self::expr::statements::kill::kill_statement_compute;
pub(crate) use self::expr::statements::live::live_statement_compute;
pub(crate) use self::expr::statements::output::output_statement_compute;
pub(crate) use self::expr::statements::rebuild::{
	rebuild_index_statement_compute, rebuild_statement_compute,
};
pub(crate) use self::expr::statements::relate::relate_statement_compute;
pub(crate) use self::expr::statements::remove::access::remove_access_statement_compute;
pub(crate) use self::expr::statements::remove::analyzer::remove_analyzer_statement_compute;
pub(crate) use self::expr::statements::remove::api::remove_api_statement_compute;
pub(crate) use self::expr::statements::remove::bucket::remove_bucket_statement_compute;
pub(crate) use self::expr::statements::remove::config::remove_config_statement_compute;
pub(crate) use self::expr::statements::remove::database::remove_database_statement_compute;
pub(crate) use self::expr::statements::remove::event::remove_event_statement_compute;
pub(crate) use self::expr::statements::remove::field::remove_field_statement_compute;
pub(crate) use self::expr::statements::remove::function::remove_function_statement_compute;
pub(crate) use self::expr::statements::remove::index::remove_index_statement_compute;
pub(crate) use self::expr::statements::remove::model::remove_model_statement_compute;
pub(crate) use self::expr::statements::remove::module::remove_module_statement_compute;
pub(crate) use self::expr::statements::remove::namespace::remove_namespace_statement_compute;
pub(crate) use self::expr::statements::remove::param::remove_param_statement_compute;
pub(crate) use self::expr::statements::remove::remove_statement_compute;
pub(crate) use self::expr::statements::remove::sequence::remove_sequence_statement_compute;
pub(crate) use self::expr::statements::remove::table::remove_table_statement_compute;
pub(crate) use self::expr::statements::remove::user::remove_user_statement_compute;
pub(crate) use self::expr::statements::select::select_statement_compute;
pub(crate) use self::expr::statements::set::set_statement_compute;
pub(crate) use self::expr::statements::show::show_statement_compute;
pub(crate) use self::expr::statements::sleep::{sleep_statement_compute, sleep_statement_sleep};
pub(crate) use self::expr::statements::subscriptions::{
	kill_database_subscriptions, kill_namespace_principal_subscriptions,
	kill_namespace_subscriptions, kill_principal_subscriptions, kill_root_principal_subscriptions,
	kill_table_subscriptions,
};
pub(crate) use self::expr::statements::update::update_statement_compute;
pub(crate) use self::expr::statements::upsert::upsert_statement_compute;
pub(crate) use self::value::closure::closure_invoke;
pub(crate) use self::value::decrement::value_decrement;
pub(crate) use self::value::del::value_del;
pub(crate) use self::value::extend::value_extend;
pub(crate) use self::value::fetch::value_fetch;
pub(crate) use self::value::get::value_get;
pub(crate) use self::value::increment::value_increment;
pub(crate) use self::value::record_id::record_id_select_document;
pub(crate) use self::value::set::value_set;
