use std::sync::Arc;

use super::Planner;
use crate::err::Error;
use crate::exec::ExecOperator;
use crate::exec::operators::ddl::{alter, define, remove};
use crate::expr::statements::alter::AlterStatement;
use crate::expr::statements::define::DefineStatement;
use crate::expr::statements::remove::RemoveStatement;

impl<'ctx> Planner<'ctx> {
	// ========================================================================
	// DEFINE
	// ========================================================================

	pub(super) async fn plan_define_statement(
		&self,
		stmt: DefineStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match stmt {
			DefineStatement::Namespace(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				let comment = Box::pin(self.physical_expr(s.comment)).await?;
				Ok(Arc::new(define::DefineNamespacePlan::new(s.kind, s.id, name, comment)))
			}
			DefineStatement::Database(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				let comment = Box::pin(self.physical_expr(s.comment)).await?;
				Ok(Arc::new(define::DefineDatabasePlan::new(
					s.kind,
					name,
					s.strict,
					comment,
					s.changefeed,
				)))
			}
			DefineStatement::Param(s) => {
				let value = Box::pin(self.physical_expr(s.value)).await?;
				let comment = Box::pin(self.physical_expr(s.comment)).await?;
				Ok(Arc::new(define::DefineParamPlan::new(
					s.kind,
					s.name,
					value,
					comment,
					s.permissions,
				)))
			}
			DefineStatement::Function(s) => {
				let comment = Box::pin(self.physical_expr(s.comment)).await?;
				Ok(Arc::new(define::DefineFunctionPlan::new(
					s.kind,
					s.name,
					s.args,
					s.block,
					s.returns,
					comment,
					s.permissions,
				)))
			}
			DefineStatement::Analyzer(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				let comment = Box::pin(self.physical_expr(s.comment)).await?;
				Ok(Arc::new(define::DefineAnalyzerPlan::new(
					s.kind,
					name,
					s.function,
					s.tokenizers,
					s.filters,
					comment,
				)))
			}
			DefineStatement::Event(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				let target_table = self.physical_expr_as_name(s.target_table).await?;
				let comment = Box::pin(self.physical_expr(s.comment)).await?;
				Ok(Arc::new(define::DefineEventPlan::new(
					s.kind,
					name,
					target_table,
					s.when,
					s.then,
					comment,
					s.event_kind,
				)))
			}
			DefineStatement::Model(s) => {
				let comment = Box::pin(self.physical_expr(s.comment)).await?;
				Ok(Arc::new(define::DefineModelPlan::new(
					s.kind,
					s.hash,
					s.name,
					s.version,
					comment,
					s.permissions,
				)))
			}
			DefineStatement::Module(s) => {
				use crate::catalog::ModuleName;
				let storage_name = ModuleName::try_from(&s)
					.map_err(|e| Error::Internal(e.to_string()))?
					.get_storage_name();
				let comment = Box::pin(self.physical_expr(s.comment)).await?;
				Ok(Arc::new(define::DefineModulePlan::new(
					s.kind,
					s.name,
					storage_name,
					s.executable,
					comment,
					s.permissions,
				)))
			}
			DefineStatement::Bucket(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				let backend = match s.backend {
					Some(expr) => Some(Box::pin(self.physical_expr(expr)).await?),
					None => None,
				};
				let comment = Box::pin(self.physical_expr(s.comment)).await?;
				Ok(Arc::new(define::DefineBucketPlan::new(
					s.kind,
					name,
					backend,
					s.permissions,
					s.readonly,
					comment,
				)))
			}
			DefineStatement::Sequence(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				let batch = Box::pin(self.physical_expr(s.batch)).await?;
				let start = Box::pin(self.physical_expr(s.start)).await?;
				let timeout = Box::pin(self.physical_expr(s.timeout)).await?;
				Ok(Arc::new(define::DefineSequencePlan::new(s.kind, name, batch, start, timeout)))
			}
			// Complex variants not yet implemented in the new planner
			DefineStatement::Table(_) => Err(Error::PlannerUnsupported(
				"DEFINE TABLE not yet supported in execution plans".to_string(),
			)),
			DefineStatement::Field(_) => Err(Error::PlannerUnsupported(
				"DEFINE FIELD not yet supported in execution plans".to_string(),
			)),
			DefineStatement::Index(_) => Err(Error::PlannerUnsupported(
				"DEFINE INDEX not yet supported in execution plans".to_string(),
			)),
			DefineStatement::User(_) => Err(Error::PlannerUnsupported(
				"DEFINE USER not yet supported in execution plans".to_string(),
			)),
			DefineStatement::Access(_) => Err(Error::PlannerUnsupported(
				"DEFINE ACCESS not yet supported in execution plans".to_string(),
			)),
			DefineStatement::Config(_) => Err(Error::PlannerUnsupported(
				"DEFINE CONFIG not yet supported in execution plans".to_string(),
			)),
			DefineStatement::Api(_) => Err(Error::PlannerUnsupported(
				"DEFINE API not yet supported in execution plans".to_string(),
			)),
		}
	}

	// ========================================================================
	// REMOVE
	// ========================================================================

	pub(super) async fn plan_remove_statement(
		&self,
		stmt: RemoveStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match stmt {
			RemoveStatement::Namespace(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				Ok(Arc::new(remove::RemoveNamespacePlan::new(name, s.if_exists, s.expunge)))
			}
			RemoveStatement::Database(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				Ok(Arc::new(remove::RemoveDatabasePlan::new(name, s.if_exists, s.expunge)))
			}
			RemoveStatement::Function(s) => {
				Ok(Arc::new(remove::RemoveFunctionPlan::new(s.name, s.if_exists)))
			}
			RemoveStatement::Analyzer(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				Ok(Arc::new(remove::RemoveAnalyzerPlan::new(name, s.if_exists)))
			}
			RemoveStatement::Access(s) => {
				use crate::exec::context::ContextLevel;
				let required_context = ContextLevel::from(s.base);
				let name = self.physical_expr_as_name(s.name).await?;
				Ok(Arc::new(remove::RemoveAccessPlan::new(
					name,
					s.base,
					s.if_exists,
					required_context,
				)))
			}
			RemoveStatement::Param(s) => {
				Ok(Arc::new(remove::RemoveParamPlan::new(s.name, s.if_exists)))
			}
			RemoveStatement::Table(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				Ok(Arc::new(remove::RemoveTablePlan::new(name, s.if_exists, s.expunge)))
			}
			RemoveStatement::Event(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				let table_name = self.physical_expr_as_name(s.table_name).await?;
				Ok(Arc::new(remove::RemoveEventPlan::new(name, table_name, s.if_exists)))
			}
			RemoveStatement::Field(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				let table_name = self.physical_expr_as_name(s.table_name).await?;
				Ok(Arc::new(remove::RemoveFieldPlan::new(name, table_name, s.if_exists)))
			}
			RemoveStatement::Index(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				let what = self.physical_expr_as_name(s.what).await?;
				Ok(Arc::new(remove::RemoveIndexPlan::new(name, what, s.if_exists)))
			}
			RemoveStatement::User(s) => {
				use crate::exec::context::ContextLevel;
				let required_context = ContextLevel::from(s.base);
				let name = self.physical_expr_as_name(s.name).await?;
				Ok(Arc::new(remove::RemoveUserPlan::new(
					name,
					s.base,
					s.if_exists,
					required_context,
				)))
			}
			RemoveStatement::Model(s) => {
				Ok(Arc::new(remove::RemoveModelPlan::new(s.name, s.version, s.if_exists)))
			}
			RemoveStatement::Api(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				Ok(Arc::new(remove::RemoveApiPlan::new(name, s.if_exists)))
			}
			RemoveStatement::Bucket(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				Ok(Arc::new(remove::RemoveBucketPlan::new(name, s.if_exists)))
			}
			RemoveStatement::Sequence(s) => {
				let name = self.physical_expr_as_name(s.name).await?;
				Ok(Arc::new(remove::RemoveSequencePlan::new(name, s.if_exists)))
			}
			RemoveStatement::Module(s) => {
				Ok(Arc::new(remove::RemoveModulePlan::new(s.name, s.if_exists)))
			}
		}
	}

	// ========================================================================
	// ALTER
	// ========================================================================

	pub(super) async fn plan_alter_statement(
		&self,
		stmt: AlterStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match stmt {
			AlterStatement::System(s) => {
				let drop_timeout =
					matches!(s.query_timeout, crate::expr::statements::alter::AlterKind::Drop);
				let timeout = match s.query_timeout {
					crate::expr::statements::alter::AlterKind::Set(expr) => {
						Some(Box::pin(self.physical_expr(expr)).await?)
					}
					_ => None,
				};
				Ok(Arc::new(alter::AlterSystemPlan::new(timeout, drop_timeout, s.compact)))
			}
			AlterStatement::Namespace(s) => Ok(Arc::new(alter::AlterNamespacePlan::new(s.compact))),
			AlterStatement::Database(s) => Ok(Arc::new(alter::AlterDatabasePlan::new(s.compact))),
			AlterStatement::Table(s) => Ok(Arc::new(alter::AlterTablePlan::new(
				s.name,
				s.if_exists,
				s.schemafull,
				s.permissions,
				s.changefeed,
				s.comment,
				s.compact,
				s.kind,
			))),
			AlterStatement::Index(s) => Ok(Arc::new(alter::AlterIndexPlan::new(
				s.name,
				s.table,
				s.if_exists,
				s.prepare_remove,
				s.comment,
			))),
			AlterStatement::Sequence(s) => {
				let timeout = match s.timeout {
					Some(expr) => Some(Box::pin(self.physical_expr(expr)).await?),
					None => None,
				};
				Ok(Arc::new(alter::AlterSequencePlan::new(s.name, s.if_exists, timeout)))
			}
			AlterStatement::Field(s) => Ok(Arc::new(alter::AlterFieldPlan::new(
				s.name,
				s.what,
				s.if_exists,
				s.kind,
				s.flexible,
				s.readonly,
				s.value,
				s.assert,
				s.default,
				s.permissions,
				s.comment,
				s.reference,
			))),
		}
	}
}
