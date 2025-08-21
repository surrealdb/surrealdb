use std::collections::{BTreeMap, HashMap};

use rust_decimal::Decimal;

use crate::{
	Array, Bytes, Datetime, Duration, File, Geometry, GeometryKind, Kind, KindLiteral, Number,
	Object, Range, RecordId, SurrealNone, SurrealNull, Uuid, Value,
};

pub trait SurrealValue {
	fn kind_of() -> Kind;
	fn is_value(value: &Value) -> bool;
	fn into_value(self) -> Value;
	fn from_value(value: Value) -> Option<Self>
	where
		Self: Sized;
}

// Simple macro for concrete types
macro_rules! impl_surreal_value_concrete {
    (
        $type:ty as $kind:expr,
        $($is_fnc:ident)? ($value_is:ident) => $is:expr,
        $($from_fnc:ident)? ($self:ident) => $into:expr,
        $($into_fnc:ident)? ($value_into:ident) => $from:expr
    ) => {
        impl SurrealValue for $type {
            fn kind_of() -> Kind {
                $kind
            }

            fn is_value($value_is: &Value) -> bool {
                $is
            }

            fn into_value($self) -> Value {
                $into
            }

            fn from_value($value_into: Value) -> Option<Self> {
                $from
            }
        }

        $(
            impl Value {
                pub fn $is_fnc(&self) -> bool {
                    <$type>::is_value(self)
                }
            }
        )?

        $(
            impl Value {
                pub fn $from_fnc(value: $type) -> Value {
                    <$type>::into_value(value)
                }
            }
        )?

        $(
            impl Value {
                pub fn $into_fnc(self) -> Option<$type> {
                    <$type>::from_value(self)
                }
            }
        )?
    };
}

// Macro for generic types
macro_rules! impl_surreal_value_generic {
    (
        <$($generic:ident),*> $type:ty as $kind:expr,
        $($is_fnc:ident<$($is_generic:ident),*>)? ($value_is:ident) => $is:expr,
        $($from_fnc:ident<$($from_generic:ident),*>)? ($self:ident) => $into:expr,
        $($into_fnc:ident<$($into_generic:ident),*>)? ($value_into:ident) => $from:expr
    ) => {
        impl<$($generic: SurrealValue),*> SurrealValue for $type {
            fn kind_of() -> Kind {
                $kind
            }

            fn is_value($value_is: &Value) -> bool {
                $is
            }

            fn into_value($self) -> Value {
                $into
            }

            fn from_value($value_into: Value) -> Option<Self> {
                $from
            }
        }

        $(
            impl Value {
                pub fn $is_fnc<$($is_generic: SurrealValue),*>(&self) -> bool {
                    <$type>::is_value(self)
                }
            }
        )?

        $(
            impl Value {
                pub fn $from_fnc<$($from_generic: SurrealValue),*>(value: $type) -> Value {
                    <$type>::into_value(value)
                }
            }
        )?

        $(
            impl Value {
                pub fn $into_fnc<$($into_generic: SurrealValue),*>(self) -> Option<$type> {
                    <$type>::from_value(self)
                }
            }
        )?
    };
}

// Concrete type implementations using the macro
impl_surreal_value_concrete!(
	Value as Kind::Any,
	(_value) => true,
	(self) => self,
	(value) => Some(value)
);

impl_surreal_value_concrete!(
	() as Kind::None,
	(value) => matches!(value, Value::None),
	(self) => Value::None,
	(value) => {
		if value == Value::None {
			Some(())
		} else {
			None
		}
	}
);

impl_surreal_value_concrete!(
	SurrealNone as Kind::None,
	is_none(value) => matches!(value, Value::None),
	(self) => Value::None,
	(value) => {
		if value == Value::None {
			Some(SurrealNone)
		} else {
			None
		}
	}
);

impl_surreal_value_concrete!(
	SurrealNull as Kind::Null,
	is_null(value) => matches!(value, Value::Null),
	(self) => Value::Null,
	(value) => {
		if value == Value::Null {
			Some(SurrealNull)
		} else {
			None
		}
	}
);

impl_surreal_value_concrete!(
	bool as Kind::Bool,
	is_bool(value) => matches!(value, Value::Bool(_)),
	from_bool(self) => Value::Bool(self),
	into_bool(value) => {
		let Value::Bool(b) = value else {
			return None;
		};
		Some(b)
	}
);

// Manual impls for true & false

impl Value {
	pub fn is_true(&self) -> bool {
		matches!(self, Value::Bool(true))
	}

	pub fn is_false(&self) -> bool {
		matches!(self, Value::Bool(false))
	}
}

impl_surreal_value_concrete!(
	Number as Kind::Number,
	is_number(value) => matches!(value, Value::Number(_)),
	from_number(self) => Value::Number(self),
	into_number(value) => {
		let Value::Number(n) = value else {
			return None;
		};
		Some(n)
	}
);

impl_surreal_value_concrete!(
	i64 as Kind::Int,
	is_int(value) => matches!(value, Value::Number(Number::Int(_))),
	from_int(self) => Value::Number(Number::Int(self)),
	into_int(value) => {
		let Value::Number(Number::Int(n)) = value else {
			return None;
		};
		Some(n)
	}
);

impl_surreal_value_concrete!(
	f64 as Kind::Float,
	is_float(value) => matches!(value, Value::Number(Number::Float(_))),
	from_float(self) => Value::Number(Number::Float(self)),
	into_float(value) => {
		let Value::Number(Number::Float(n)) = value else {
			return None;
		};
		Some(n)
	}
);

impl_surreal_value_concrete!(
	Decimal as Kind::Decimal,
	is_decimal(value) => matches!(value, Value::Number(Number::Decimal(_))),
	from_decimal(self) => Value::Number(Number::Decimal(self)),
	into_decimal(value) => {
		let Value::Number(Number::Decimal(n)) = value else {
			return None;
		};
		Some(n)
	}
);

impl_surreal_value_concrete!(
	String as Kind::String,
	is_string(value) => matches!(value, Value::String(_)),
	from_string(self) => Value::String(self),
	into_string(value) => {
		let Value::String(s) = value else {
			return None;
		};
		Some(s)
	}
);

impl_surreal_value_concrete!(
	Duration as Kind::Duration,
	is_duration(value) => matches!(value, Value::Duration(_)),
	from_duration(self) => Value::Duration(self),
	into_duration(value) => {
		let Value::Duration(d) = value else {
			return None;
		};
		Some(d)
	}
);

impl_surreal_value_concrete!(
	std::time::Duration as Kind::Duration,
	(value) => matches!(value, Value::Duration(_)),
	(self) => Value::Duration(Duration(self)),
	(value) => {
		let Value::Duration(Duration(d)) = value else {
			return None;
		};
		Some(d)
	}
);

impl_surreal_value_concrete!(
	Datetime as Kind::Datetime,
	is_datetime(value) => matches!(value, Value::Datetime(_)),
	from_datetime(self) => Value::Datetime(self),
	into_datetime(value) => {
		let Value::Datetime(d) = value else {
			return None;
		};
		Some(d)
	}
);

impl_surreal_value_concrete!(
	chrono::DateTime<chrono::Utc> as Kind::Datetime,
	(value) => matches!(value, Value::Datetime(_)),
	(self) => Value::Datetime(Datetime(self)),
	(value) => {
		let Value::Datetime(Datetime(d)) = value else {
			return None;
		};
		Some(d)
	}
);

impl_surreal_value_concrete!(
	Uuid as Kind::Uuid,
	is_uuid(value) => matches!(value, Value::Uuid(_)),
	from_uuid(self) => Value::Uuid(self),
	into_uuid(value) => {
		let Value::Uuid(u) = value else {
			return None;
		};
		Some(u)
	}
);

impl_surreal_value_concrete!(
	uuid::Uuid as Kind::Uuid,
	(value) => matches!(value, Value::Uuid(_)),
	(self) => Value::Uuid(Uuid(self)),
	(value) => {
		let Value::Uuid(Uuid(u)) = value else {
			return None;
		};
		Some(u)
	}
);

impl_surreal_value_concrete!(
	Array as Kind::Array(Box::new(Kind::Any), None),
	is_array(value) => matches!(value, Value::Array(_)),
	from_array(self) => Value::Array(self),
	into_array(value) => {
		let Value::Array(a) = value else {
			return None;
		};
		Some(a)
	}
);

impl_surreal_value_concrete!(
	Object as Kind::Object,
	is_object(value) => matches!(value, Value::Object(_)),
	from_object(self) => Value::Object(self),
	into_object(value) => {
		let Value::Object(o) = value else {
			return None;
		};
		Some(o)
	}
);

impl_surreal_value_concrete!(
	Geometry as Kind::Geometry(vec![]),
	is_geometry(value) => matches!(value, Value::Geometry(_)),
	from_geometry(self) => Value::Geometry(self),
	into_geometry(value) => {
		let Value::Geometry(g) = value else {
			return None;
		};
		Some(g)
	}
);

impl_surreal_value_concrete!(
	Bytes as Kind::Bytes,
	is_bytes(value) => matches!(value, Value::Bytes(_)),
	from_bytes(self) => Value::Bytes(self),
	into_bytes(value) => {
		let Value::Bytes(b) = value else {
			return None;
		};
		Some(b)
	}
);

impl_surreal_value_concrete!(
	Vec<u8> as Kind::Bytes,
	(value) => matches!(value, Value::Bytes(_)),
	(self) => Value::Bytes(Bytes(self)),
	(value) => {
		let Value::Bytes(Bytes(b)) = value else {
			return None;
		};
		Some(b)
	}
);

impl_surreal_value_concrete!(
	bytes::Bytes as Kind::Bytes,
	(value) => matches!(value, Value::Bytes(_)),
	(self) => Value::Bytes(Bytes(self.to_vec())),
	(value) => {
		let Value::Bytes(Bytes(b)) = value else {
			return None;
		};
		Some(bytes::Bytes::from(b))
	}
);

impl_surreal_value_concrete!(
	RecordId as Kind::Record(vec![]),
	is_record(value) => matches!(value, Value::RecordId(_)),
	from_record(self) => Value::RecordId(self),
	into_record(value) => {
		let Value::RecordId(r) = value else {
			return None;
		};
		Some(r)
	}
);

impl_surreal_value_concrete!(
	File as Kind::File(vec![]),
	is_file(value) => matches!(value, Value::File(_)),
	from_file(self) => Value::File(self),
	into_file(value) => {
		let Value::File(f) = value else {
			return None;
		};
		Some(f)
	}
);

impl_surreal_value_concrete!(
	Range as Kind::Range,
	is_range(value) => matches!(value, Value::Range(_)),
	from_range(self) => Value::Range(Box::new(self)),
	into_range(value) => {
		let Value::Range(r) = value else {
			return None;
		};
		Some(*r)
	}
);

// Generic implementations using the macro
impl_surreal_value_generic!(
	<T> Vec<T> as Kind::Array(Box::new(T::kind_of()), None),
	is_vec<T>(value) => {
		if let Value::Array(Array(a)) = value {
			a.iter().all(T::is_value)
		} else {
			false
		}
	},
	from_vec<T>(self) => Value::Array(Array(self.into_iter().map(SurrealValue::into_value).collect())),
	into_vec<T>(value) => {
		let Value::Array(Array(a)) = value else {
			return None;
		};
		a.into_iter().map(|v| T::from_value(v)).collect()
	}
);

impl_surreal_value_generic!(
	<T> Option<T> as Kind::Option(Box::new(T::kind_of())),
	is_option<T>(value) => matches!(value, Value::None) || T::is_value(value),
	from_option<T>(self) => self.map(T::into_value).unwrap_or(Value::None),
	into_option<T>(value) => match value{
		Value::None => Some(None),
		x => T::from_value(x).map(Some),
	}
);

impl_surreal_value_generic!(
	<V> BTreeMap<String, V> as Kind::Object,
	(value) => {
		if let Value::Object(Object(o)) = value {
			o.iter().all(|(_, v)| V::is_value(v))
		} else {
			false
		}
	},
	from_btreemap<V>(self) => Value::Object(Object(self.into_iter().map(|(k, v)| (k, v.into_value())).collect())),
	into_btreemap<V>(value) => {
		let Value::Object(Object(o)) = value else {
			return None;
		};
		o.into_iter().map(|(k, v)| V::from_value(v).map(|v| (k, v))).collect()
	}
);

impl_surreal_value_generic!(
	<V> HashMap<String, V> as Kind::Object,
	(value) => {
		if let Value::Object(Object(o)) = value {
			o.iter().all(|(_, v)| V::is_value(v))
		} else {
			false
		}
	},
	from_hashmap<V>(self) => Value::Object(Object(self.into_iter().map(|(k, v)| (k, v.into_value())).collect())),
	into_hashmap<V>(value) => {
		let Value::Object(Object(o)) = value else {
			return None;
		};
		o.into_iter().map(|(k, v)| V::from_value(v).map(|v| (k, v))).collect()
	}
);

// Geometry implementations
impl_surreal_value_concrete!(
	geo::Point as Kind::Geometry(vec![GeometryKind::Point]),
	is_point(value) => matches!(value, Value::Geometry(Geometry::Point(_))),
	from_point(self) => Value::Geometry(Geometry::Point(self)),
	into_point(value) => {
		let Value::Geometry(Geometry::Point(p)) = value else {
			return None;
		};
		Some(p)
	}
);

impl_surreal_value_concrete!(
	geo::LineString as Kind::Geometry(vec![GeometryKind::Line]),
	is_line(value) => matches!(value, Value::Geometry(Geometry::Line(_))),
	from_line(self) => Value::Geometry(Geometry::Line(self)),
	into_line(value) => {
		let Value::Geometry(Geometry::Line(l)) = value else {
			return None;
		};
		Some(l)
	}
);

impl_surreal_value_concrete!(
	geo::Polygon as Kind::Geometry(vec![GeometryKind::Polygon]),
	is_polygon(value) => matches!(value, Value::Geometry(Geometry::Polygon(_))),
	from_polygon(self) => Value::Geometry(Geometry::Polygon(self)),
	into_polygon(value) => {
		let Value::Geometry(Geometry::Polygon(p)) = value else {
			return None;
		};
		Some(p)
	}
);

impl_surreal_value_concrete!(
	geo::MultiPoint as Kind::Geometry(vec![GeometryKind::MultiPoint]),
	is_multipoint(value) => matches!(value, Value::Geometry(Geometry::MultiPoint(_))),
	from_multipoint(self) => Value::Geometry(Geometry::MultiPoint(self)),
	into_multipoint(value) => {
		let Value::Geometry(Geometry::MultiPoint(m)) = value else {
			return None;
		};
		Some(m)
	}
);

impl_surreal_value_concrete!(
	geo::MultiLineString as Kind::Geometry(vec![GeometryKind::MultiLine]),
	is_multiline(value) => matches!(value, Value::Geometry(Geometry::MultiLine(_))),
	from_multiline(self) => Value::Geometry(Geometry::MultiLine(self)),
	into_multiline(value) => {
		let Value::Geometry(Geometry::MultiLine(m)) = value else {
			return None;
		};
		Some(m)
	}
);

impl_surreal_value_concrete!(
	geo::MultiPolygon as Kind::Geometry(vec![GeometryKind::MultiPolygon]),
	is_multipolygon(value) => matches!(value, Value::Geometry(Geometry::MultiPolygon(_))),
	from_multipolygon(self) => Value::Geometry(Geometry::MultiPolygon(self)),
	into_multipolygon(value) => {
		let Value::Geometry(Geometry::MultiPolygon(m)) = value else {
			return None;
		};
		Some(m)
	}
);

// Tuple implementations
macro_rules! impl_tuples {
    ($($n:expr => ($($t:ident),+)),+ $(,)?) => {
        $(
            impl<$($t: SurrealValue),+> SurrealValue for ($($t,)+) {
                fn kind_of() -> Kind {
                    Kind::Literal(KindLiteral::Array(vec![$($t::kind_of()),+]))
                }

                fn is_value(value: &Value) -> bool {
                    if let Value::Array(Array(a)) = value {
                        a.len() == $n && {
                            let mut iter = a.iter();
                            $(
                                iter.next().map_or(false, |v| $t::is_value(v))
                            )&&*
                        }
                    } else {
                        false
                    }
                }

                fn into_value(self) -> Value {
                    #[allow(non_snake_case)]
                    let ($($t,)+) = self;
                    Value::Array(Array(vec![$($t.into_value()),+]))
                }

                fn from_value(value: Value) -> Option<Self> {
                    let Value::Array(Array(mut a)) = value else {
                        return None;
                    };

                    if a.len() != $n {
                        return None;
                    }

                    $(#[allow(non_snake_case)] let $t = $t::from_value(a.remove(0))?;)+
                    Some(($($t,)+))
                }
            }
        )+
    }
}

impl_tuples! {
	1 => (A),
	2 => (A, B),
	3 => (A, B, C),
	4 => (A, B, C, D),
	5 => (A, B, C, D, E),
	6 => (A, B, C, D, E, F),
	7 => (A, B, C, D, E, F, G),
	8 => (A, B, C, D, E, F, G, H),
	9 => (A, B, C, D, E, F, G, H, I),
	10 => (A, B, C, D, E, F, G, H, I, J)
}
