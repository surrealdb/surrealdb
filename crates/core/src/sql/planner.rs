
use crate::{err::Error, expr::LogicalPlan, sql::statement::Statement};

pub struct SqlToLogical {

}

impl SqlToLogical {
    pub fn new() -> Self {
        Self {}
    }

    pub fn statement_to_logical_plan(&self, statement: Statement) -> Result<LogicalPlan, Error> {
        match statement {
            Statement::Value(v) => self.value_to_logical_expr(v),
            Statement::Analyze(stmt) => self.analyze_to_logical_expr(stmt),
            Statement::Begin(stmt) => self.begin_to_logical_expr(stmt),
            Statement::Break(stmt) => self.break_to_logical_expr(stmt),
            Statement::Continue(stmt) => self.continue_to_logical_expr(stmt),
            Statement::Cancel(stmt) => self.cancel_to_logical_expr(stmt),
            Statement::Commit(stmt) => self.commit_to_logical_expr(stmt),
            Statement::Create(stmt) => self.create_to_logical_expr(stmt),
            Statement::Define(stmt) => self.define_to_logical_expr(stmt),
            Statement::Delete(stmt) => self.delete_to_logical_expr(stmt),
            Statement::Foreach(stmt) => self.foreach_to_logical_expr(stmt),
            Statement::Ifelse(stmt) => self.ifelse_to_logical_expr(stmt),
            Statement::Info(stmt) => self.info_to_logical_expr(stmt),
            Statement::Insert(stmt) => self.insert_to_logical_expr(stmt),
            Statement::Kill(stmt) => self.kill_to_logical_expr(stmt),
            Statement::Live(stmt) => self.live_to_logical_expr(stmt),
            Statement::Option(stmt) => self.option_to_logical_expr(stmt),
            Statement::Output(stmt) => self.output_to_logical_expr(stmt),
            Statement::Relate(stmt) => self.relate_to_logical_expr(stmt),
            Statement::Remove(stmt) => self.remove_to_logical_expr(stmt),
            Statement::Select(stmt) => self.select_to_logical_expr(stmt),
            Statement::Set(stmt) => self.set_to_logical_expr(stmt),
            Statement::Show(stmt) => self.show_to_logical_expr(stmt),
            Statement::Sleep(stmt) => self.sleep_to_logical_expr(stmt),
            Statement::Update(stmt) => self.update_to_logical_expr(stmt),
            Statement::Throw(stmt) => self.throw_to_logical_expr(stmt),
            Statement::Use(stmt) => self.use_to_logical_expr(stmt),
            Statement::Rebuild(stmt) => self.rebuild_to_logical_expr(stmt),
            Statement::Upsert(stmt) => self.upsert_to_logical_expr(stmt),
            Statement::Alter(stmt) => self.alter_to_logical_expr(stmt),
            Statement::Access(stmt) => self.access_to_logical_expr(stmt),
        }
    }
}
