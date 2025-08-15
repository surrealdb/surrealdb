use std::{collections::BTreeMap, ops::Bound, sync::LazyLock};

use rust_decimal::Decimal;
use surrealdb_core::val::{
	Array, Bytes, Closure, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId,
	RecordIdKey, RecordIdKeyRange, Regex, Strand, Table, Uuid, Value,
};

#[derive(Debug, Clone)]
pub struct RoughlyEqConfig {
	pub record_id_keys: bool,
	pub uuid: bool,
	pub datetime: bool,
	pub float: bool,
	pub decimal: bool,
}

impl RoughlyEqConfig {
	pub fn all() -> Self {
		RoughlyEqConfig {
			record_id_keys: true,
			uuid: true,
			datetime: true,
			float: false,
			decimal: false,
		}
	}
}

pub trait RoughlyEq {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool;
}

impl<T: RoughlyEq> RoughlyEq for Vec<T> {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		if self.len() != other.len() {
			return false;
		}
		self.iter().zip(other.iter()).all(|(a, b)| a.roughly_equal(b, config))
	}
}
impl<T: RoughlyEq> RoughlyEq for Box<T> {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		T::roughly_equal(self, other, config)
	}
}
impl<K: RoughlyEq, V: RoughlyEq> RoughlyEq for BTreeMap<K, V> {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		if self.len() != other.len() {
			return false;
		}
		self.iter().zip(other.iter()).all(|((ak, av), (bk, bv))| {
			K::roughly_equal(ak, bk, config) && V::roughly_equal(av, bv, config)
		})
	}
}

impl<T: RoughlyEq> RoughlyEq for Bound<T> {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		match (self, other) {
			(Bound::Unbounded, Bound::Unbounded) => true,
			(Bound::Included(a), Bound::Included(b)) => a.roughly_equal(b, config),
			(Bound::Excluded(a), Bound::Excluded(b)) => a.roughly_equal(b, config),
			_ => false,
		}
	}
}

macro_rules! impl_roughly_eq_delegate {
    ($($name:ident),*) => {
        $(
        impl RoughlyEq for $name {
            fn roughly_equal(&self, other: &Self, _: &RoughlyEqConfig) -> bool {
                self == other
            }
        }
        )*
    };
}

macro_rules! impl_roughly_eq_struct {
    ($name:ident, $($t:tt),* $(,)?) => {
        impl RoughlyEq for $name{
            fn roughly_equal(&self, other: &Self,  config: &RoughlyEqConfig) -> bool{
                true $(&&
                    self.$t.roughly_equal(&other.$t,config)
                )*
            }
        }
    };
}

macro_rules! impl_roughly_eq_enum {
    ($name:ident{ $($variant:ident$(($($field:ident),* $(,)?))?),* $(,)? }) => {
        impl RoughlyEq for $name {
            fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
                $(
                    impl_roughly_eq_enum!(@match_pattern self,other, $name, config,$variant$(($($field,)*))?);
                )*

                false
            }
        }
    };


    (@match_pattern $this:expr_2021, $other:expr_2021, $ty_name:ident, $config:ident, $name:ident) => {
        if let $ty_name::$name = $this{
            if let $ty_name::$name = $other{
                return true
            }
            return false
        }
    };

    (@match_pattern $this:expr_2021, $other:expr_2021, $ty_name:ident, $config:ident, $name:ident($($field:ident),* $(,)?)) => {
        #[expect(non_camel_case_types)]
        struct $name<$($field,)*> {
            $($field: $field),*
        }

        if let $ty_name::$name($($field),*) = $this{
            let __tmp = $name{
                $($field,)*
            };
            if let $ty_name::$name($($field),*) = $other {
                return true $(&& __tmp.$field.roughly_equal($field,$config))*;
            }else{
                return false
            }

        }
    };

}

/// The point of roughly eq,
/// don't check the parts of id that are often randomly generated.
///
/// Do check if they are of the same type.
impl RoughlyEq for RecordIdKey {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		if config.record_id_keys {
			match (self, other) {
				(RecordIdKey::Number(a), RecordIdKey::Number(b)) => a == b,
				(RecordIdKey::String(a), RecordIdKey::String(b)) => a == b,
				(RecordIdKey::Uuid(a), RecordIdKey::Uuid(b)) => {
					if config.uuid {
						a == b
					} else {
						true
					}
				}
				(RecordIdKey::Array(a), RecordIdKey::Array(b)) => a.roughly_equal(b, config),
				(RecordIdKey::Object(a), RecordIdKey::Object(b)) => a.roughly_equal(b, config),
				(RecordIdKey::Range(a), RecordIdKey::Range(b)) => a.roughly_equal(b, config),
				_ => false,
			}
		} else {
			match (self, other) {
				(RecordIdKey::Number(_), RecordIdKey::Number(_)) => true,
				(RecordIdKey::String(_), RecordIdKey::String(_)) => true,
				(RecordIdKey::Uuid(_), RecordIdKey::Uuid(_)) => true,
				(RecordIdKey::Array(a), RecordIdKey::Array(b)) => a.roughly_equal(b, config),
				(RecordIdKey::Object(a), RecordIdKey::Object(b)) => a.roughly_equal(b, config),
				(RecordIdKey::Range(a), RecordIdKey::Range(b)) => a.roughly_equal(b, config),
				_ => false,
			}
		}
	}
}

impl RoughlyEq for Datetime {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		if config.datetime {
			self == other
		} else {
			true
		}
	}
}

impl RoughlyEq for Uuid {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		if config.uuid {
			self == other
		} else {
			true
		}
	}
}

/// Tolerance for floating point comparisons.
const EPSILON: f64 = 1e-15;
/// Tolerance for decimal comparisons (1e-15).
const EPSILON_DECIMAL: LazyLock<Decimal> = LazyLock::new(|| Decimal::try_new(1, 15).unwrap());

impl RoughlyEq for f64 {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		if config.float {
			(self - other).abs() < EPSILON
		} else {
			self == other
		}
	}
}

impl RoughlyEq for Decimal {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		if config.decimal {
			(self - other).abs() < *EPSILON_DECIMAL
		} else {
			self == other
		}
	}
}

impl RoughlyEq for Number {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		if self.is_nan() && other.is_nan() {
			return true;
		}

		match (self, other) {
			(Number::Float(a), Number::Float(b)) => a.roughly_equal(b, config),
			(Number::Decimal(a), Number::Decimal(b)) => a.roughly_equal(b, config),
			(a, b) => a == b,
		}
	}
}

impl_roughly_eq_delegate!(
	i64, bool, String, Closure, Geometry, Bytes, Range, Regex, Duration, File, Strand, Table
);

impl_roughly_eq_struct!(Array, 0);
impl_roughly_eq_struct!(Object, 0);
impl_roughly_eq_struct!(RecordId, table, key);
impl_roughly_eq_struct!(RecordIdKeyRange, start, end);

impl_roughly_eq_enum!(
	Value{
		None,
		Null,
		Bool(b),
		Number(n),
		Strand(s),
		Duration(d),
		Datetime(d),
		Uuid(u),
		Array(a),
		Object(o),
		Geometry(g),
		Bytes(b),
		RecordId(t),
		Table(t),
		Regex(r),
		Range(r),
		Closure(c),
		File(f),
	}
);
