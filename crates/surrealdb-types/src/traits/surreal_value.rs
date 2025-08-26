use std::collections::{BTreeMap, HashMap};

use rust_decimal::Decimal;

use crate::{
	Array, Bytes, Datetime, Duration, File, Geometry, GeometryKind, Kind, KindLiteral, Number,
	Object, Range, RecordId, SurrealNone, SurrealNull, Uuid, Value,
};

/// Trait for converting between SurrealDB values and Rust types
///
/// This trait provides a type-safe way to convert between SurrealDB `Value` types
/// and Rust types. It defines four main operations:
/// - `kind_of()`: Returns the `Kind` that represents this type
/// - `is_value()`: Checks if a `Value` can be converted to this type
/// - `into_value()`: Converts this type into a `Value`
/// - `from_value()`: Attempts to convert a `Value` into this type
pub trait SurrealValue {
	/// Returns the kind that represents this type
	fn kind_of() -> Kind;
	/// Checks if the given value can be converted to this type
	fn is_value(value: &Value) -> bool;
	/// Converts this type into a SurrealDB value
	fn into_value(self) -> Value;
	/// Attempts to convert a SurrealDB value into this type
	fn from_value(value: Value) -> anyhow::Result<Self>
	where
		Self: Sized;
}

macro_rules! impl_surreal_value {
	// Generic types
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

            fn from_value($value_into: Value) -> anyhow::Result<Self> {
                $from
            }
        }

        $(
            impl Value {
                /// Checks if this value can be converted to the given type
                ///
                /// Returns `true` if the value can be converted to the type, `false` otherwise.
                pub fn $is_fnc<$($is_generic: SurrealValue),*>(&self) -> bool {
                    <$type>::is_value(self)
                }
            }
        )?

        $(
            impl Value {
                /// Converts the given value into a `Value`
                pub fn $from_fnc<$($from_generic: SurrealValue),*>(value: $type) -> Value {
                    <$type>::into_value(value)
                }
            }
        )?

        $(
            impl Value {
                /// Attempts to convert a SurrealDB value into the given type
                ///
                /// Returns `Ok(T)` if the conversion is successful, `Err(anyhow::Error)` otherwise.
                pub fn $into_fnc<$($into_generic: SurrealValue),*>(self) -> anyhow::Result<$type> {
                    <$type>::from_value(self)
                }
            }
        )?
    };

	// Concrete types
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

            fn from_value($value_into: Value) -> anyhow::Result<Self> {
                $from
            }
        }

        $(
            impl Value {
                /// Checks if this value can be converted to the given type
                ///
                /// Returns `true` if the value can be converted to the type, `false` otherwise.
                pub fn $is_fnc(&self) -> bool {
                    <$type>::is_value(self)
                }
            }
        )?

        $(
            impl Value {
                /// Converts the given value into a `Value`
                pub fn $from_fnc(value: $type) -> Value {
                    <$type>::into_value(value)
                }
            }
        )?

        $(
            impl Value {
                /// Attempts to convert a SurrealDB value into the given type
                ///
                /// Returns `Ok(T)` if the conversion is successful, `Err(anyhow::Error)` otherwise.
                pub fn $into_fnc(self) -> anyhow::Result<$type> {
                    <$type>::from_value(self)
                }
            }
        )?
    };
}

// Concrete type implementations using the macro
impl_surreal_value!(
	Value as Kind::Any,
	(_value) => true,
	(self) => self,
	(value) => Ok(value)
);

impl_surreal_value!(
	() as Kind::None,
	(value) => matches!(value, Value::None),
	(self) => Value::None,
	(value) => {
		if value == Value::None {
			Ok(())
		} else {
			Err(convertion_error(Self::kind_of(), value))
		}
	}
);

impl_surreal_value!(
	SurrealNone as Kind::None,
	is_none(value) => matches!(value, Value::None),
	(self) => Value::None,
	(value) => {
		if value == Value::None {
			Ok(SurrealNone)
		} else {
			Err(convertion_error(Self::kind_of(), value))
		}
	}
);

impl_surreal_value!(
	SurrealNull as Kind::Null,
	is_null(value) => matches!(value, Value::Null),
	(self) => Value::Null,
	(value) => {
		if value == Value::Null {
			Ok(SurrealNull)
		} else {
			Err(convertion_error(Self::kind_of(), value))
		}
	}
);

impl_surreal_value!(
	bool as Kind::Bool,
	is_bool(value) => matches!(value, Value::Bool(_)),
	from_bool(self) => Value::Bool(self),
	into_bool(value) => {
		let Value::Bool(b) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(b)
	}
);

// Manual impls for true & false

impl Value {
	/// Checks if this value is specifically `true`
	///
	/// Returns `true` if the value is `Value::Bool(true)`, `false` otherwise.
	pub fn is_true(&self) -> bool {
		matches!(self, Value::Bool(true))
	}

	/// Checks if this value is specifically `false`
	///
	/// Returns `true` if the value is `Value::Bool(false)`, `false` otherwise.
	pub fn is_false(&self) -> bool {
		matches!(self, Value::Bool(false))
	}
}

impl_surreal_value!(
	Number as Kind::Number,
	is_number(value) => matches!(value, Value::Number(_)),
	from_number(self) => Value::Number(self),
	into_number(value) => {
		let Value::Number(n) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(n)
	}
);

impl_surreal_value!(
	i64 as Kind::Int,
	is_int(value) => matches!(value, Value::Number(Number::Int(_))),
	from_int(self) => Value::Number(Number::Int(self)),
	into_int(value) => {
		let Value::Number(Number::Int(n)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(n)
	}
);

impl_surreal_value!(
	f64 as Kind::Float,
	is_float(value) => matches!(value, Value::Number(Number::Float(_))),
	from_float(self) => Value::Number(Number::Float(self)),
	into_float(value) => {
		let Value::Number(Number::Float(n)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(n)
	}
);

impl_surreal_value!(
	Decimal as Kind::Decimal,
	is_decimal(value) => matches!(value, Value::Number(Number::Decimal(_))),
	from_decimal(self) => Value::Number(Number::Decimal(self)),
	into_decimal(value) => {
		let Value::Number(Number::Decimal(n)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(n)
	}
);

impl_surreal_value!(
	String as Kind::String,
	is_string(value) => matches!(value, Value::String(_)),
	from_string(self) => Value::String(self),
	into_string(value) => {
		let Value::String(s) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(s)
	}
);

impl_surreal_value!(
	Duration as Kind::Duration,
	is_duration(value) => matches!(value, Value::Duration(_)),
	from_duration(self) => Value::Duration(self),
	into_duration(value) => {
		let Value::Duration(d) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(d)
	}
);

impl_surreal_value!(
	std::time::Duration as Kind::Duration,
	(value) => matches!(value, Value::Duration(_)),
	(self) => Value::Duration(Duration(self)),
	(value) => {
		let Value::Duration(Duration(d)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(d)
	}
);

impl_surreal_value!(
	Datetime as Kind::Datetime,
	is_datetime(value) => matches!(value, Value::Datetime(_)),
	from_datetime(self) => Value::Datetime(self),
	into_datetime(value) => {
		let Value::Datetime(d) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(d)
	}
);

impl_surreal_value!(
	chrono::DateTime<chrono::Utc> as Kind::Datetime,
	(value) => matches!(value, Value::Datetime(_)),
	(self) => Value::Datetime(Datetime(self)),
	(value) => {
		let Value::Datetime(Datetime(d)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(d)
	}
);

impl_surreal_value!(
	Uuid as Kind::Uuid,
	is_uuid(value) => matches!(value, Value::Uuid(_)),
	from_uuid(self) => Value::Uuid(self),
	into_uuid(value) => {
		let Value::Uuid(u) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(u)
	}
);

impl_surreal_value!(
	uuid::Uuid as Kind::Uuid,
	(value) => matches!(value, Value::Uuid(_)),
	(self) => Value::Uuid(Uuid(self)),
	(value) => {
		let Value::Uuid(Uuid(u)) = value else {
			return Err(convertion_error(<Uuid>::kind_of(), value));
		};
		Ok(u)
	}
);

impl_surreal_value!(
	Array as Kind::Array(Box::new(Kind::Any), None),
	is_array(value) => matches!(value, Value::Array(_)),
	from_array(self) => Value::Array(self),
	into_array(value) => {
		let Value::Array(a) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(a)
	}
);

impl_surreal_value!(
	Object as Kind::Object,
	is_object(value) => matches!(value, Value::Object(_)),
	from_object(self) => Value::Object(self),
	into_object(value) => {
		let Value::Object(o) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(o)
	}
);

impl_surreal_value!(
	Geometry as Kind::Geometry(vec![]),
	is_geometry(value) => matches!(value, Value::Geometry(_)),
	from_geometry(self) => Value::Geometry(self),
	into_geometry(value) => {
		let Value::Geometry(g) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(g)
	}
);

impl_surreal_value!(
	Bytes as Kind::Bytes,
	is_bytes(value) => matches!(value, Value::Bytes(_)),
	from_bytes(self) => Value::Bytes(self),
	into_bytes(value) => {
		let Value::Bytes(b) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(b)
	}
);

impl_surreal_value!(
	Vec<u8> as Kind::Bytes,
	(value) => matches!(value, Value::Bytes(_)),
	(self) => Value::Bytes(Bytes(self)),
	(value) => {
		let Value::Bytes(Bytes(b)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(b)
	}
);

impl_surreal_value!(
	bytes::Bytes as Kind::Bytes,
	(value) => matches!(value, Value::Bytes(_)),
	(self) => Value::Bytes(Bytes(self.to_vec())),
	(value) => {
		let Value::Bytes(Bytes(b)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(bytes::Bytes::from(b))
	}
);

impl_surreal_value!(
	RecordId as Kind::Record(vec![]),
	is_record(value) => matches!(value, Value::RecordId(_)),
	from_record(self) => Value::RecordId(self),
	into_record(value) => {
		let Value::RecordId(r) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(r)
	}
);

impl_surreal_value!(
	File as Kind::File(vec![]),
	is_file(value) => matches!(value, Value::File(_)),
	from_file(self) => Value::File(self),
	into_file(value) => {
		let Value::File(f) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(f)
	}
);

impl_surreal_value!(
	Range as Kind::Range,
	is_range(value) => matches!(value, Value::Range(_)),
	from_range(self) => Value::Range(Box::new(self)),
	into_range(value) => {
		let Value::Range(r) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(*r)
	}
);

// Generic implementations using the macro
impl_surreal_value!(
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
			return Err(convertion_error(Self::kind_of(), value));
		};
		a
			.into_iter()
			.map(|v| T::from_value(v))
			.collect::<anyhow::Result<Vec<T>>>()
			.map_err(|e| anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))
	}
);

impl_surreal_value!(
	<T> Option<T> as Kind::Option(Box::new(T::kind_of())),
	is_option<T>(value) => matches!(value, Value::None) || T::is_value(value),
	from_option<T>(self) => self.map(T::into_value).unwrap_or(Value::None),
	into_option<T>(value) => match value{
		Value::None => Ok(None),
		x => T::from_value(x).map(Some).map_err(|e| anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e)),
	}
);

impl_surreal_value!(
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
			return Err(convertion_error(Self::kind_of(), value));
		};
		o
			.into_iter()
			.map(|(k, v)| V::from_value(v).map(|v| (k, v)))
			.collect::<anyhow::Result<BTreeMap<String, V>>>()
			.map_err(|e| anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))
	}
);

impl_surreal_value!(
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
			return Err(convertion_error(Self::kind_of(), value));
		};
		o
			.into_iter()
			.map(|(k, v)| V::from_value(v).map(|v| (k, v)))
			.collect::<anyhow::Result<HashMap<String, V>>>()
			.map_err(|e| anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))
	}
);

// Geometry implementations
impl_surreal_value!(
	geo::Point as Kind::Geometry(vec![GeometryKind::Point]),
	is_point(value) => matches!(value, Value::Geometry(Geometry::Point(_))),
	from_point(self) => Value::Geometry(Geometry::Point(self)),
	into_point(value) => {
		let Value::Geometry(Geometry::Point(p)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(p)
	}
);

impl_surreal_value!(
	geo::LineString as Kind::Geometry(vec![GeometryKind::Line]),
	is_line(value) => matches!(value, Value::Geometry(Geometry::Line(_))),
	from_line(self) => Value::Geometry(Geometry::Line(self)),
	into_line(value) => {
		let Value::Geometry(Geometry::Line(l)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(l)
	}
);

impl_surreal_value!(
	geo::Polygon as Kind::Geometry(vec![GeometryKind::Polygon]),
	is_polygon(value) => matches!(value, Value::Geometry(Geometry::Polygon(_))),
	from_polygon(self) => Value::Geometry(Geometry::Polygon(self)),
	into_polygon(value) => {
		let Value::Geometry(Geometry::Polygon(p)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(p)
	}
);

impl_surreal_value!(
	geo::MultiPoint as Kind::Geometry(vec![GeometryKind::MultiPoint]),
	is_multipoint(value) => matches!(value, Value::Geometry(Geometry::MultiPoint(_))),
	from_multipoint(self) => Value::Geometry(Geometry::MultiPoint(self)),
	into_multipoint(value) => {
		let Value::Geometry(Geometry::MultiPoint(m)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(m)
	}
);

impl_surreal_value!(
	geo::MultiLineString as Kind::Geometry(vec![GeometryKind::MultiLine]),
	is_multiline(value) => matches!(value, Value::Geometry(Geometry::MultiLine(_))),
	from_multiline(self) => Value::Geometry(Geometry::MultiLine(self)),
	into_multiline(value) => {
		let Value::Geometry(Geometry::MultiLine(m)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(m)
	}
);

impl_surreal_value!(
	geo::MultiPolygon as Kind::Geometry(vec![GeometryKind::MultiPolygon]),
	is_multipolygon(value) => matches!(value, Value::Geometry(Geometry::MultiPolygon(_))),
	from_multipolygon(self) => Value::Geometry(Geometry::MultiPolygon(self)),
	into_multipolygon(value) => {
		let Value::Geometry(Geometry::MultiPolygon(m)) = value else {
			return Err(convertion_error(Self::kind_of(), value));
		};
		Ok(m)
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

                fn from_value(value: Value) -> anyhow::Result<Self> {
                    let Value::Array(Array(mut a)) = value else {
                        return Err(convertion_error(Self::kind_of(), value));
                    };

                    if a.len() != $n {
                        return Err(anyhow::anyhow!("Failed to convert to {}: Expected Array of length {}, got {}", Self::kind_of(), $n, a.len()));
                    }

                    $(#[allow(non_snake_case)] let $t = $t::from_value(a.remove(0)).map_err(|e| anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))?;)+
                    Ok(($($t,)+))
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

fn convertion_error(expected: Kind, got: Value) -> anyhow::Error {
	anyhow::anyhow!("Expected {}, got {:?}", expected, got.value_kind())
}
