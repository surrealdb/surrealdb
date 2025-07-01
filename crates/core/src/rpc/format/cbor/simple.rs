use crate::sql::{Number, SqlValue};

use super::{decode::Decoder, err::Error};

macro_rules! create_simple {
    ($($name:ident($len:expr)),* $(,)?) => {
        #[derive(PartialEq, Eq, Clone, Copy, Debug)]
        pub enum Simple {
            $($name,)*
        }

        impl TryFrom<u8> for Simple {
            type Error = Error;
            fn try_from(len: u8) -> Result<Simple, Error> {
                match len {
                    $($len => Ok(Simple::$name),)*
                    31 => Err(Error::UnexpectedBreak),
                    len => Err(Error::InvalidSimpleValue(len))
                }
            }
        }

        impl From<Simple> for u8 {
            fn from(len: Simple) -> u8 {
                match len {
                    $(Simple::$name => $len,)*
                }
            }
        }

        impl From<Simple> for u64 {
            fn from(len: Simple) -> u64 {
                match len {
                    $(Simple::$name => $len,)*
                }
            }
        }
    };
}

create_simple!(False(20), True(21), Null(22), Undefined(23), F16(25), F32(26), F64(27),);

impl Simple {
	pub fn decode(&self, dec: &mut Decoder) -> Result<SqlValue, Error> {
		match self {
			Simple::False => Ok(SqlValue::Bool(false)),
			Simple::True => Ok(SqlValue::Bool(true)),
			Simple::Null => Ok(SqlValue::Null),
			Simple::Undefined => Ok(SqlValue::None),
			Simple::F16 => Ok(SqlValue::Number(Number::Float(dec.0.read_f16()?.to_f64()))),
			Simple::F32 => Ok(SqlValue::Number(Number::Float(dec.0.read_f32()? as f64))),
			Simple::F64 => Ok(SqlValue::Number(Number::Float(dec.0.read_f64()?))),
		}
	}
}
