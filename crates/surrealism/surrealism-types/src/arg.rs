use surrealdb_types::SurrealValue;

pub struct SerializableArg<T: SurrealValue>(pub T);
impl<T: SurrealValue> From<T> for SerializableArg<T> {
    fn from(value: T) -> Self {
        SerializableArg(value)
    }
}