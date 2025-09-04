use crate::val::record::Record;
use crate::val::Value;

#[derive(Clone, Debug)]
pub enum Output {
    /// A single record from the database
    Record(Record),
    /// A single value (could be any Value type)
    Value(Value),
    /// An array of output items (recursive)
    Array(Vec<Output>),
}

impl Output {
    pub(crate) fn into_value(self) -> Value {
        match self {
            Output::Record(record) => record.data.into_value(),
            Output::Value(value) => value,
            Output::Array(array) => array.into_iter().map(Self::into_value).collect(),
        }
    }
}