use std::collections::BTreeMap;

use crate::{Bytes, Datetime, Duration, Number, Object, Value};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::{Kind, Strand, SurrealNone, SurrealNull, Uuid};

pub trait KindOf {
    fn kind_of() -> Kind;
}

// NONE

impl KindOf for () {
    fn kind_of() -> Kind {
        Kind::None
    }
}

impl KindOf for SurrealNone {
    fn kind_of() -> Kind {
        Kind::None
    }
}

// NULL

impl KindOf for SurrealNull {
    fn kind_of() -> Kind {
        Kind::Null
    }
}

// BOOL

impl KindOf for bool {
    fn kind_of() -> Kind {
        Kind::Bool
    }
}

// BYTES

impl KindOf for Vec<u8> {
    fn kind_of() -> Kind {
        Kind::Bytes
    }
}

impl KindOf for Bytes {
    fn kind_of() -> Kind {
        Kind::Bytes
    }
}

impl KindOf for bytes::Bytes {
    fn kind_of() -> Kind {
        Kind::Bytes
    }
}

// DATETIME

impl KindOf for Datetime {
    fn kind_of() -> Kind {
        Kind::Datetime
    }
}

impl KindOf for DateTime<Utc> {
    fn kind_of() -> Kind {
        Kind::Datetime
    }
}

// DECIMAL

impl KindOf for Decimal {
    fn kind_of() -> Kind {
        Kind::Decimal
    }
}

// DURATION

impl KindOf for Duration {
    fn kind_of() -> Kind {
        Kind::Duration
    }
}

impl KindOf for std::time::Duration {
    fn kind_of() -> Kind {
        Kind::Duration
    }
}

// FLOAT

impl KindOf for f64 {
    fn kind_of() -> Kind {
        Kind::Float
    }
}

// INT

impl KindOf for i64 {
    fn kind_of() -> Kind {
        Kind::Int
    }
}

// NUMBER

impl KindOf for Number {
    fn kind_of() -> Kind {
        Kind::Number
    }
}

// OBJECT

impl KindOf for BTreeMap<String, Value> {
    fn kind_of() -> Kind {
        Kind::Object
    }
}

impl KindOf for Object {
    fn kind_of() -> Kind {
        Kind::Object
    }
}

// 