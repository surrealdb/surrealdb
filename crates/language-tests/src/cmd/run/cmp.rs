use std::{collections::BTreeMap, ops::Bound};

use rust_decimal::Decimal;
use surrealdb_core::sql::{
	Array, Block, Bytes, Cast, Closure, Constant, Datetime, Duration, Edges, Expression, Function,
	Future, Geometry, Id, IdRange, Idiom, Mock, Model, Number, Object, Param, Query, Range, Regex,
	Subquery, Thing, Uuid, Value,
};

pub trait RoughlyEq {
	fn roughly_equal(&self, other: &Self) -> bool;
}

impl<T: RoughlyEq> RoughlyEq for Vec<T> {
	fn roughly_equal(&self, other: &Self) -> bool {
		if self.len() != other.len() {
			return false;
		}
		self.iter().zip(other.iter()).all(|(a, b)| a.roughly_equal(b))
	}
}
impl<T: RoughlyEq> RoughlyEq for Box<T> {
	fn roughly_equal(&self, other: &Self) -> bool {
		T::roughly_equal(self, other)
	}
}
impl<K: RoughlyEq, V: RoughlyEq> RoughlyEq for BTreeMap<K, V> {
	fn roughly_equal(&self, other: &Self) -> bool {
		if self.len() != other.len() {
			return false;
		}
		self.iter()
			.zip(other.iter())
			.all(|((ak, av), (bk, bv))| K::roughly_equal(ak, bk) && V::roughly_equal(av, bv))
	}
}

impl<T: RoughlyEq> RoughlyEq for Bound<T> {
	fn roughly_equal(&self, other: &Self) -> bool {
		match (self, other) {
			(Bound::Unbounded, Bound::Unbounded) => true,
			(Bound::Included(a), Bound::Included(b)) => a.roughly_equal(b),
			(Bound::Excluded(a), Bound::Excluded(b)) => a.roughly_equal(b),
			_ => false,
		}
	}
}

macro_rules! impl_roughly_eq_delegate {
    ($($name:ident),*) => {
        $(
        impl RoughlyEq for $name {
            fn roughly_equal(&self, other: &Self) -> bool {
                self == other
            }
        }
        )*
    };
}

macro_rules! impl_roughly_eq_struct {
    ($name:ident, $($t:tt),* $(,)?) => {
        impl RoughlyEq for $name{
            fn roughly_equal(&self, other: &Self) -> bool{
                true $(&&
                    self.$t.roughly_equal(&other.$t)
                )*
            }
        }
    };
}

macro_rules! impl_roughly_eq_enum {
    ($name:ident{ $($variant:ident$(($($field:ident),* $(,)?))?),* $(,)? }) => {
        impl RoughlyEq for $name {
            fn roughly_equal(&self, other: &Self) -> bool {
                $(
                    impl_roughly_eq_enum!(@match_pattern self,other, $name, $variant$(($($field,)*))?);
                )*

                false
            }
        }
    };


    (@match_pattern $this:expr, $other:expr, $ty_name:ident, $name:ident) => {
        if let $ty_name::$name = $this{
            if let $ty_name::$name = $other{
                return true
            }
            return false
        }
    };

    (@match_pattern $this:expr, $other:expr, $ty_name:ident, $name:ident($($field:ident),* $(,)?)) => {
        #[allow(non_camel_case_types)]
        struct $name<$($field,)*> {
            $($field: $field),*
        }

        if let $ty_name::$name($(ref $field),*) = $this{
            let __tmp = $name{
                $($field,)*
            };
            if let $ty_name::$name($(ref $field),*) = $other {
                return true $(&& __tmp.$field.roughly_equal($field))*;
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
	fn roughly_equal(&self, other: &Self) -> bool {
		match (self, other) {
			(Id::Number(_), Id::Number(_)) => true,
			(Id::String(_), Id::String(_)) => true,
			(Id::Uuid(_), Id::Uuid(_)) => true,
			(Id::Array(a), Id::Array(b)) => a.roughly_equal(b),
			(Id::Object(a), Id::Object(b)) => a.roughly_equal(b),
			(Id::Generate(a), Id::Generate(b)) => a == b,
			(Id::Range(a), Id::Range(b)) => a.roughly_equal(b),
			_ => false,
		}
	}
}

impl RoughlyEq for Datetime {
	fn roughly_equal(&self, _other: &Self) -> bool {
		true
	}
}

impl_roughly_eq_delegate!(
	i64, f64, Decimal, Query, bool, String, Closure, Expression, Uuid, Number, Geometry, Bytes,
	Param, Model, Subquery, Function, Constant, Future, Edges, Range, Block, Cast, Regex, Mock,
	Idiom, Duration
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
