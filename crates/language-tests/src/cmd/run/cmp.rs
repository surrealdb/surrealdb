use std::{collections::BTreeMap, ops::Bound};

use rust_decimal::Decimal;
use surrealdb_core::sql::{
	Array, Block, Bytes, Cast, Closure, Constant, Datetime, Duration, Edges, Expression, Function,
	Future, Geometry, Id, IdRange, Idiom, Mock, Model, Number, Object, Param, Query, Range, Regex,
	Subquery, Thing, Uuid, Value,
};

#[derive(Debug, Clone)]
pub struct RoughlyEqConfig {
	pub record_id_keys: bool,
	pub uuid: bool,
	pub datetime: bool,
}

impl RoughlyEqConfig {
	pub fn all() -> Self {
		RoughlyEqConfig {
			record_id_keys: true,
			uuid: true,
			datetime: true,
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


    (@match_pattern $this:expr, $other:expr, $ty_name:ident, $config:ident, $name:ident) => {
        if let $ty_name::$name = $this{
            if let $ty_name::$name = $other{
                return true
            }
            return false
        }
    };

    (@match_pattern $this:expr, $other:expr, $ty_name:ident, $config:ident, $name:ident($($field:ident),* $(,)?)) => {
        #[allow(non_camel_case_types)]
        struct $name<$($field,)*> {
            $($field: $field),*
        }

        if let $ty_name::$name($(ref $field),*) = $this{
            let __tmp = $name{
                $($field,)*
            };
            if let $ty_name::$name($(ref $field),*) = $other {
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
impl RoughlyEq for Id {
	fn roughly_equal(&self, other: &Self, config: &RoughlyEqConfig) -> bool {
		if config.record_id_keys {
			match (self, other) {
				(Id::Number(a), Id::Number(b)) => a == b,
				(Id::String(a), Id::String(b)) => a == b,
				(Id::Uuid(a), Id::Uuid(b)) => {
					if config.uuid {
						a == b
					} else {
						true
					}
				}
				(Id::Array(a), Id::Array(b)) => a.roughly_equal(b, config),
				(Id::Object(a), Id::Object(b)) => a.roughly_equal(b, config),
				(Id::Generate(a), Id::Generate(b)) => a == b,
				(Id::Range(a), Id::Range(b)) => a.roughly_equal(b, config),
				_ => false,
			}
		} else {
			match (self, other) {
				(Id::Number(_), Id::Number(_)) => true,
				(Id::String(_), Id::String(_)) => true,
				(Id::Uuid(_), Id::Uuid(_)) => true,
				(Id::Array(a), Id::Array(b)) => a.roughly_equal(b, config),
				(Id::Object(a), Id::Object(b)) => a.roughly_equal(b, config),
				(Id::Generate(a), Id::Generate(b)) => a == b,
				(Id::Range(a), Id::Range(b)) => a.roughly_equal(b, config),
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

impl_roughly_eq_delegate!(
	i64, f64, Decimal, Query, bool, String, Closure, Expression, Number, Geometry, Bytes, Param,
	Model, Subquery, Function, Constant, Future, Edges, Range, Block, Cast, Regex, Mock, Idiom,
	Duration
);

impl_roughly_eq_struct!(Array, 0);
impl_roughly_eq_struct!(Object, 0);
impl_roughly_eq_struct!(Thing, tb, id);
impl_roughly_eq_struct!(IdRange, beg, end);

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
	Thing(t),
	Param(p),
	Idiom(i),
	Table(t),
	Mock(m),
	Regex(r),
	Cast(c),
	Block(b),
	Range(r),
	Edges(e),
	Future(f),
	Constant(c),
	Function(f),
	Subquery(s),
	Expression(s),
	Query(q),
	Model(m),
	Closure(c),
	}
);
