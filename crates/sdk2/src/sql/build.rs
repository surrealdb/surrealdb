use surrealdb_types::{SurrealValue, Variables};
use uuid::Uuid;

pub(crate) trait BuildSql {
    fn build(self, ctx: &mut BuildSqlContext);
}

impl BuildSql for &str {
    fn build(self, ctx: &mut BuildSqlContext) {
        ctx.sql.push_str(self);
    }
}

impl BuildSql for String {
    fn build(self, ctx: &mut BuildSqlContext) {
        ctx.sql.push_str(&self);
    }
}

impl BuildSql for char {
    fn build(self, ctx: &mut BuildSqlContext) {
        ctx.sql.push(self);
    }
}

impl BuildSql for &char {
    fn build(self, ctx: &mut BuildSqlContext) {
        ctx.sql.push(*self);
    }
}

#[derive(Default)]
pub(crate) struct BuildSqlContext {
    pub(crate) sql: String,
    pub(crate) vars: Variables,
}

impl BuildSqlContext {
    pub(crate) fn push(&mut self, buildable: impl BuildSql) {
        buildable.build(self);
    }

    pub(crate) fn var(&mut self, value: impl SurrealValue) -> String {
        let uuid_str = Uuid::new_v4().to_string().replace('-', "");
        let key = format!("var_{}", &uuid_str[..8]);
        self.vars.insert(key.to_string(), value);
        format!("${key}")
    }

    pub(crate) fn output(self) -> (String, Variables) {
        (self.sql, self.vars)
    }
}