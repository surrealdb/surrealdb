use crate::{dbs::Session, sql::{Object, Value}};
use super::method::Method;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ApiInvocation<'a> {
    pub params: Object,
    pub body: Value,
    pub method: Method,
    pub query: Object,
    pub session: Option<Session>,
    pub values: Vec<(&'a str, Value)>
}

impl<'a> Into<Value> for ApiInvocation<'a> {
    fn into(self) -> Value {
        let mut obj = map! {
            "params" => Value::from(self.params),
            "body" => self.body,
            "method" => self.method.to_string().into(),
            "query" => Value::from(self.query),
        };

        if let Some(session) = self.session {
            obj.extend(
                session
                    .values()
                    .into_iter()
            );
        }

        obj.extend(self.values.into_iter());

        obj.into()
    }
}