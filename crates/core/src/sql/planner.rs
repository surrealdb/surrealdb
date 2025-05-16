use crate::{err::Error, expr::LogicalPlan, sql::statement::Statement};

pub struct SqlToLogical {}

impl SqlToLogical {
	pub fn new() -> Self {
		Self {}
	}

	/// Convert a SQL statement to a logical plan
	pub fn statement_to_logical_plan(&self, statement: Statement) -> Result<LogicalPlan, Error> {
		Ok(match statement {
			Statement::Value(v) => LogicalPlan::Value(v.into()),
			Statement::Analyze(v) => LogicalPlan::Analyze(v.into()),
			Statement::Begin(v) => LogicalPlan::Begin(v.into()),
			Statement::Break(v) => LogicalPlan::Break(v.into()),
			Statement::Continue(v) => LogicalPlan::Continue(v.into()),
			Statement::Cancel(v) => LogicalPlan::Cancel(v.into()),
			Statement::Commit(v) => LogicalPlan::Commit(v.into()),
			Statement::Create(v) => LogicalPlan::Create(v.into()),
			Statement::Define(v) => LogicalPlan::Define(v.into()),
			Statement::Delete(v) => LogicalPlan::Delete(v.into()),
			Statement::Foreach(v) => LogicalPlan::Foreach(v.into()),
			Statement::Ifelse(v) => LogicalPlan::Ifelse(v.into()),
			Statement::Info(v) => LogicalPlan::Info(v.into()),
			Statement::Insert(v) => LogicalPlan::Insert(v.into()),
			Statement::Kill(v) => LogicalPlan::Kill(v.into()),
			Statement::Live(v) => LogicalPlan::Live(v.into()),
			Statement::Option(v) => LogicalPlan::Option(v.into()),
			Statement::Output(v) => LogicalPlan::Output(v.into()),
			Statement::Relate(v) => LogicalPlan::Relate(v.into()),
			Statement::Remove(v) => LogicalPlan::Remove(v.into()),
			Statement::Select(v) => LogicalPlan::Select(v.into()),
			Statement::Set(v) => LogicalPlan::Set(v.into()),
			Statement::Show(v) => LogicalPlan::Show(v.into()),
			Statement::Sleep(v) => LogicalPlan::Sleep(v.into()),
			Statement::Update(v) => LogicalPlan::Update(v.into()),
			Statement::Throw(v) => LogicalPlan::Throw(v.into()),
			Statement::Use(v) => LogicalPlan::Use(v.into()),
			Statement::Rebuild(v) => LogicalPlan::Rebuild(v.into()),
			Statement::Upsert(v) => LogicalPlan::Upsert(v.into()),
			Statement::Alter(v) => LogicalPlan::Alter(v.into()),
			Statement::Access(v) => LogicalPlan::Access(v.into()),
		})
	}
}
