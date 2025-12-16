use crate::sql::{BuildSql, BuildSqlContext, Fields, IntoFields};

#[derive(Clone)]
pub enum Return {
    None,
    Null,
    Diff,
    After,
    Before,
    Fields(Fields),
}

impl BuildSql for Return {
    fn build(self, ctx: &mut BuildSqlContext) {
        match self {
            Return::None => ctx.push("NONE"),
            Return::Null => ctx.push("NULL"),
            Return::Diff => ctx.push("DIFF"),
            Return::After => ctx.push("AFTER"),
            Return::Before => ctx.push("BEFORE"),
            Return::Fields(fields) => fields.build(ctx),
        }
    }
}

pub struct ReturnBuilder;

impl ReturnBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn none(self) -> Return {
        Return::None
    }

    pub fn null(self) -> Return {
        Return::Null
    }
    
    pub fn diff(self) -> Return {
        Return::Diff
    }

    pub fn after(self) -> Return {
        Return::After
    }
    
    pub fn before(self) -> Return {
        Return::Before
    }

    pub fn fields<T: IntoFields>(self, fields: T) -> Return {
        let mut x = Fields::default();
        IntoFields::build(fields, &mut x);
        Return::Fields(x.into())
    }
}