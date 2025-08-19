use crate::Value;

pub trait SurrealValue {
    fn into_surreal_value(self) -> Value;
    fn from_surreal_value(value: Value) -> anyhow::Result<Self>
        where Self: Sized;
}

impl SurrealValue for Value {
    fn into_surreal_value(self) -> Value {
        self
    }

    fn from_surreal_value(value: Value) -> anyhow::Result<Self> {
        Ok(value)
    }
}

impl SurrealValue for () {
    fn into_surreal_value(self) -> Value {
        Value::None
    }

    fn from_surreal_value(value: Value) -> anyhow::Result<Self> {
        if value {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Expected None, got {:?}", value))
        }
    }
}

impl SurrealValue for bool {
    fn into_surreal_value(self) -> Value {
        Value::Bool(self)
    }

    fn from_surreal_value(value: Value) -> anyhow::Result<Self> {
        let Value::Bool(b) = value else {
            return Err(anyhow::anyhow!("Expected bool, got {:?}", value));
        };

        Ok(b)
    }
}
