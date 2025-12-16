use crate::sql::{BuildSql, BuildSqlContext};

#[derive(Default, Clone)]
pub struct Fields(pub Vec<String>);

impl Fields {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl BuildSql for Fields {
    fn build(self, ctx: &mut BuildSqlContext) {
        if self.0.is_empty() {
            ctx.push("*");
        } else {
            let var = ctx.var(self.0); 
            ctx.push(format!("type::fields({var})"));
        }
    }
}

pub trait IntoFields {
    fn build(self, fields: &mut Fields);
}

impl IntoFields for &str {
    fn build(self, fields: &mut Fields) {
        fields.0.push(self.to_string());
    }
}

impl IntoFields for String {
    fn build(self, fields: &mut Fields) {
        fields.0.push(self);
    }
}

impl IntoFields for Vec<&str> {
    fn build(self, fields: &mut Fields) {
        fields.0.extend(self.into_iter().map(Into::into));
    }
}

impl IntoFields for Vec<String> {
    fn build(self, fields: &mut Fields) {
        fields.0.extend(self);
    }
}

impl<const N: usize> IntoFields for [&str; N] {
    fn build(self, fields: &mut Fields) {
        fields.0.extend(self.into_iter().map(Into::into));
    }
}