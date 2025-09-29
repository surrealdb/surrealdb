use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
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
        $($into_fnc:ident<$($into_generic:ident),*>)? ($value_into:ident) => $from:expr $(,)?
		$(,$is_and_fnc:ident<$($is_and_generic:ident),*> ($self_is_and:ident, $is_and_callback:ident) => $is_and:expr)?
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

		$(
			impl Value {
				/// Checks if this value can be converted to the given type
				///
				/// Returns `true` if the value can be converted to the type, `false` otherwise.
				pub fn $is_and_fnc<$($is_and_generic: SurrealValue),*>(&$self_is_and, $is_and_callback: impl FnOnce(&$type) -> bool) -> bool {
					$is_and
				}
			}
		)?
    };

	// Concrete types
    (
        $type:ty as $kind:expr,
        $($is_fnc:ident)? ($value_is:ident) => $is:expr,
        $($from_fnc:ident)? ($self:ident) => $into:expr,
        $($into_fnc:ident)? ($value_into:ident) => $from:expr $(,)?
		$(,$is_and_fnc:ident($self_is_and:ident, $is_and_callback:ident) => $is_and:expr)?
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

		$(
			impl Value {
				/// Checks if this value can be converted to the given type
				///
				/// Returns `true` if the value can be converted to the type, `false` otherwise.
				pub fn $is_and_fnc(&$self_is_and, $is_and_callback: impl FnOnce(&$type) -> bool) -> bool {
					$is_and
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
			Err(conversion_error(Self::kind_of(), value))
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
			Err(conversion_error(Self::kind_of(), value))
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
			Err(conversion_error(Self::kind_of(), value))
		}
	}
);

impl_surreal_value!(
	bool as Kind::Bool,
	is_bool(value) => matches!(value, Value::Bool(_)),
	from_bool(self) => Value::Bool(self),
	into_bool(value) => {
		let Value::Bool(b) = value else {
			return Err(conversion_error(Self::kind_of(), value));
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
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(n)
	},
	is_number_and(self, callback) => {
		if let Value::Number(n) = self {
			callback(n)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	i64 as Kind::Int,
	is_int(value) => matches!(value, Value::Number(Number::Int(_))),
	from_int(self) => Value::Number(Number::Int(self)),
	into_int(value) => {
		let Value::Number(Number::Int(n)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(n)
	},
	is_int_and(self, callback) => {
		if let Value::Number(Number::Int(n)) = self {
			callback(n)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	f64 as Kind::Float,
	is_float(value) => matches!(value, Value::Number(Number::Float(_))),
	from_float(self) => Value::Number(Number::Float(self)),
	into_float(value) => {
		let Value::Number(Number::Float(n)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(n)
	},
	is_float_and(self, callback) => {
		if let Value::Number(Number::Float(n)) = self {
			callback(n)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Decimal as Kind::Decimal,
	is_decimal(value) => matches!(value, Value::Number(Number::Decimal(_))),
	from_decimal(self) => Value::Number(Number::Decimal(self)),
	into_decimal(value) => {
		let Value::Number(Number::Decimal(n)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(n)
	},
	is_decimal_and(self, callback) => {
		if let Value::Number(Number::Decimal(n)) = self {
			callback(n)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	String as Kind::String,
	is_string(value) => matches!(value, Value::String(_)),
	from_string(self) => Value::String(self),
	into_string(value) => {
		let Value::String(s) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(s)
	},
	is_string_and(self, callback) => {
		if let Value::String(s) = self {
			callback(s)
		} else {
			false
		}
	}
);

impl SurrealValue for Cow<'static, str> {
	fn kind_of() -> Kind {
		Kind::String
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::String(_))
	}

	fn into_value(self) -> Value {
		Value::String(self.to_string())
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		let Value::String(s) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(Cow::Owned(s))
	}
}

impl SurrealValue for &'static str {
	fn kind_of() -> Kind {
		Kind::String
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::String(_))
	}

	fn into_value(self) -> Value {
		Value::String(self.to_string())
	}

	fn from_value(_value: Value) -> anyhow::Result<Self> {
		Err(anyhow::anyhow!("Cannot deserialize &'static str"))
	}
}

impl_surreal_value!(
	Duration as Kind::Duration,
	is_duration(value) => matches!(value, Value::Duration(_)),
	from_duration(self) => Value::Duration(self),
	into_duration(value) => {
		let Value::Duration(d) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(d)
	},
	is_duration_and(self, callback) => {
		if let Value::Duration(d) = self {
			callback(d)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	std::time::Duration as Kind::Duration,
	(value) => matches!(value, Value::Duration(_)),
	(self) => Value::Duration(Duration(self)),
	(value) => {
		let Value::Duration(Duration(d)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
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
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(d)
	},
	is_datetime_and(self, callback) => {
		if let Value::Datetime(d) = self {
			callback(d)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	chrono::DateTime<chrono::Utc> as Kind::Datetime,
	(value) => matches!(value, Value::Datetime(_)),
	(self) => Value::Datetime(Datetime(self)),
	(value) => {
		let Value::Datetime(Datetime(d)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
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
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(u)
	},
	is_uuid_and(self, callback) => {
		if let Value::Uuid(u) = self {
			callback(u)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	uuid::Uuid as Kind::Uuid,
	(value) => matches!(value, Value::Uuid(_)),
	(self) => Value::Uuid(Uuid(self)),
	(value) => {
		let Value::Uuid(Uuid(u)) = value else {
			return Err(conversion_error(<Uuid>::kind_of(), value));
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
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(a)
	},
	is_array_and(self, callback) => {
		if let Value::Array(a) = self {
			callback(a)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Object as Kind::Object,
	is_object(value) => matches!(value, Value::Object(_)),
	from_object(self) => Value::Object(self),
	into_object(value) => {
		let Value::Object(o) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(o)
	},
	is_object_and(self, callback) => {
		if let Value::Object(o) = self {
			callback(o)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Geometry as Kind::Geometry(vec![]),
	is_geometry(value) => matches!(value, Value::Geometry(_)),
	from_geometry(self) => Value::Geometry(self),
	into_geometry(value) => {
		let Value::Geometry(g) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(g)
	},
	is_geometry_and(self, callback) => {
		if let Value::Geometry(g) = self {
			callback(g)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Bytes as Kind::Bytes,
	is_bytes(value) => matches!(value, Value::Bytes(_)),
	from_bytes(self) => Value::Bytes(self),
	into_bytes(value) => {
		let Value::Bytes(b) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(b)
	},
	is_bytes_and(self, callback) => {
		if let Value::Bytes(b) = self {
			callback(b)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Vec<u8> as Kind::Bytes,
	(value) => matches!(value, Value::Bytes(_)),
	(self) => Value::Bytes(Bytes(self)),
	(value) => {
		let Value::Bytes(Bytes(b)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
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
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(bytes::Bytes::from(b))
	}
);

impl_surreal_value!(
	RecordId as Kind::Record(vec![]),
	is_record(value) => {
		match value {
			Value::RecordId(_) => true,
			Value::String(s) => Self::parse_simple(s).is_ok(),
			Value::Object(o) => {
				o.get("id").is_some()
			}
			_ => false,
		}
	},
	from_record(self) => Value::RecordId(self),
	into_record(value) => {
		match value {
			Value::RecordId(r) => Ok(r),
			Value::String(s) => Ok(Self::parse_simple(&s)?),
			Value::Object(o) => {
				let v = o.get("id").context("Invalid record id: {value}")?;
				v.clone().into_record()
			}
			Value::Array(a) => {
				let first = a.first().context("Invalid record id: {value}")?;
				first.clone().into_record()
			}
			_ => Err(anyhow::anyhow!("Invalid record id: {value}")),
		}
	},
	is_record_and(self, callback) => {
		if let Value::RecordId(r) = self {
			callback(r)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	File as Kind::File(vec![]),
	is_file(value) => matches!(value, Value::File(_)),
	from_file(self) => Value::File(self),
	into_file(value) => {
		let Value::File(f) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(f)
	},
	is_file_and(self, callback) => {
		if let Value::File(f) = self {
			callback(f)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Range as Kind::Range,
	is_range(value) => matches!(value, Value::Range(_)),
	from_range(self) => Value::Range(Box::new(self)),
	into_range(value) => {
		let Value::Range(r) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(*r)
	},
	is_range_and(self, callback) => {
		if let Value::Range(r) = self {
			callback(r)
		} else {
			false
		}
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
			return Err(conversion_error(Self::kind_of(), value));
		};
		a
			.into_iter()
			.map(|v| T::from_value(v))
			.collect::<anyhow::Result<Vec<T>>>()
			.map_err(|e| anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))
	}
);

impl_surreal_value!(
	<T> Option<T> as Kind::Either(vec![Kind::None, T::kind_of()]),
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
			return Err(conversion_error(Self::kind_of(), value));
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
			return Err(conversion_error(Self::kind_of(), value));
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
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(p)
	},
	is_point_and(self, callback) => {
		if let Value::Geometry(Geometry::Point(p)) = self {
			callback(p)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::LineString as Kind::Geometry(vec![GeometryKind::Line]),
	is_line(value) => matches!(value, Value::Geometry(Geometry::Line(_))),
	from_line(self) => Value::Geometry(Geometry::Line(self)),
	into_line(value) => {
		let Value::Geometry(Geometry::Line(l)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(l)
	},
	is_line_and(self, callback) => {
		if let Value::Geometry(Geometry::Line(l)) = self {
			callback(l)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::Polygon as Kind::Geometry(vec![GeometryKind::Polygon]),
	is_polygon(value) => matches!(value, Value::Geometry(Geometry::Polygon(_))),
	from_polygon(self) => Value::Geometry(Geometry::Polygon(self)),
	into_polygon(value) => {
		let Value::Geometry(Geometry::Polygon(p)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(p)
	},
	is_polygon_and(self, callback) => {
		if let Value::Geometry(Geometry::Polygon(p)) = self {
			callback(p)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::MultiPoint as Kind::Geometry(vec![GeometryKind::MultiPoint]),
	is_multipoint(value) => matches!(value, Value::Geometry(Geometry::MultiPoint(_))),
	from_multipoint(self) => Value::Geometry(Geometry::MultiPoint(self)),
	into_multipoint(value) => {
		let Value::Geometry(Geometry::MultiPoint(m)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(m)
	},
	is_multipoint_and(self, callback) => {
		if let Value::Geometry(Geometry::MultiPoint(m)) = self {
			callback(m)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::MultiLineString as Kind::Geometry(vec![GeometryKind::MultiLine]),
	is_multiline(value) => matches!(value, Value::Geometry(Geometry::MultiLine(_))),
	from_multiline(self) => Value::Geometry(Geometry::MultiLine(self)),
	into_multiline(value) => {
		let Value::Geometry(Geometry::MultiLine(m)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(m)
	},
	is_multiline_and(self, callback) => {
		if let Value::Geometry(Geometry::MultiLine(m)) = self {
			callback(m)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::MultiPolygon as Kind::Geometry(vec![GeometryKind::MultiPolygon]),
	is_multipolygon(value) => matches!(value, Value::Geometry(Geometry::MultiPolygon(_))),
	from_multipolygon(self) => Value::Geometry(Geometry::MultiPolygon(self)),
	into_multipolygon(value) => {
		let Value::Geometry(Geometry::MultiPolygon(m)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(m)
	},
	is_multipolygon_and(self, callback) => {
		if let Value::Geometry(Geometry::MultiPolygon(m)) = self {
			callback(m)
		} else {
			false
		}
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
                        return Err(conversion_error(Self::kind_of(), value));
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

fn conversion_error(expected: Kind, got: Value) -> anyhow::Error {
	anyhow::anyhow!("Expected {}, got {:?}", expected, got.kind())
}

/// Non-standard numeric implementations, such as u8, u16, u32, u64, u128, i8, i16, i32, usize, f32,
/// f64,
macro_rules! impl_numeric {
	($($k:ty => $variant:ident($type:ty)),+ $(,)?) => {
		$(
			impl SurrealValue for $k {
				fn kind_of() -> Kind {
					Kind::Number
				}

				fn is_value(value: &Value) -> bool {
					matches!(value, Value::Number(_))
				}

				fn into_value(self) -> Value {
					Value::Number(Number::$variant(self as $type))
				}

				fn from_value(value: Value) -> anyhow::Result<Self> {
					let Value::Number(Number::$variant(n)) = value else {
						return Err(conversion_error(Self::kind_of(), value));
					};
					Ok(n as $k)
				}
			}
		)+
	}
}

impl_numeric! {
	i8 => Int(i64),
	i16 => Int(i64),
	i32 => Int(i64),
	isize => Int(i64),
	u16 => Int(i64),
	u32 => Int(i64),
	usize => Int(i64),
	f32 => Float(f64),
}

impl SurrealValue for serde_json::Value {
	fn kind_of() -> Kind {
		Kind::Any
	}

	fn is_value(_value: &Value) -> bool {
		true
	}

	fn into_value(self) -> Value {
		match self {
			serde_json::Value::Null => Value::Null,
			serde_json::Value::Bool(b) => Value::Bool(b),
			serde_json::Value::Number(n) => {
				if let Some(i) = n.as_i64() {
					Value::Number(Number::Int(i))
				} else if let Some(f) = n.as_f64() {
					Value::Number(Number::Float(f))
				} else {
					// If we can't extract as i64 or f64, try parsing as decimal
					Value::String(n.to_string())
				}
			}
			serde_json::Value::String(s) => Value::String(s),
			serde_json::Value::Object(o) => {
				let mut obj = Object::new();
				for (k, v) in o {
					obj.insert(k, serde_json::Value::into_value(v));
				}
				Value::Object(obj)
			}
			serde_json::Value::Array(a) => {
				Value::Array(Array(a.into_iter().map(serde_json::Value::into_value).collect()))
			}
		}
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		match value {
			Value::None | Value::Null => Ok(serde_json::Value::Null),
			Value::Bool(b) => Ok(serde_json::Value::Bool(b)),
			Value::Number(n) => match n {
				Number::Int(i) => Ok(serde_json::Value::Number(serde_json::Number::from(i))),
				Number::Float(f) => {
					if let Some(num) = serde_json::Number::from_f64(f) {
						Ok(serde_json::Value::Number(num))
					} else {
						Ok(serde_json::Value::Null)
					}
				}
				Number::Decimal(d) => Ok(serde_json::Value::String(d.to_string())),
			},
			Value::String(s) => Ok(serde_json::Value::String(s)),
			Value::Object(o) => {
				let mut obj = serde_json::Map::new();
				for (k, v) in o.0 {
					obj.insert(k, serde_json::Value::from_value(v)?);
				}
				Ok(serde_json::Value::Object(obj))
			}
			Value::Array(a) => {
				let mut arr = Vec::new();
				for v in a.0 {
					arr.push(serde_json::Value::from_value(v)?);
				}
				Ok(serde_json::Value::Array(arr))
			}
			_ => Err(conversion_error(Self::kind_of(), value)),
		}
	}
}

macro_rules! impl_slice {
	($($n:expr),+ $(,)?) => {
		$(
			impl<T: SurrealValue> SurrealValue for [T; $n] {
				fn kind_of() -> Kind {
					Kind::Array(Box::new(T::kind_of()), Some($n))
				}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::Array(_))
				}

				fn into_value(self) -> Value {
					Value::Array(Array(self.into_iter().map(T::into_value).collect()))
				}

				fn from_value(value: Value) -> anyhow::Result<Self> {
					let Value::Array(Array(a)) = value else {
						return Err(conversion_error(Self::kind_of(), value));
					};
					if a.len() != $n {
						return Err(anyhow::anyhow!("Expected array of length {}, got {}", $n, a.len()));
					}
					let mut result = Vec::with_capacity($n);
					for v in a {
						result.push(T::from_value(v)?);
					}
					result.try_into()
						.map_err(|v: Vec<_>| anyhow::anyhow!("Failed to convert vec of length {} to array of size {}", v.len(), $n))
				}
			}
		)+
	}
}

impl_slice!(
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
	27, 28, 29, 30, 31, 32
);

impl SurrealValue for http::HeaderMap {
	fn kind_of() -> Kind {
		Kind::Object
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::Object(_))
	}

	fn into_value(self) -> Value {
		let mut next_key = None;
		let mut next_value = Value::None;
		let mut first_value = true;
		let mut res = BTreeMap::new();

		// Header map can contain multiple values for each header.
		// This is handled by returning the key name first and then return multiple
		// values with key name = None.
		for (k, v) in self.into_iter() {
			let v = match v.to_str() {
				Ok(v) => Value::String(v.to_owned()),
				Err(_) => continue,
			};

			if let Some(k) = k {
				let k = k.as_str().to_owned();
				// new key, if we had accumulated a key insert it first and then update
				// accumulated state.
				if let Some(k) = next_key.take() {
					let v = std::mem::replace(&mut next_value, Value::None);
					res.insert(k, v);
				}
				next_key = Some(k);
				next_value = v;
				first_value = true;
			} else if first_value {
				// no new key, but this is directly after the first value, turn the header value
				// into an array of values.
				first_value = false;
				next_value = Value::Array(vec![next_value, v].into())
			} else {
				// Since it is not a new key and a new value it must be atleast a third header
				// value and `next_value` is already updated to an array.
				if let Value::Array(ref mut array) = next_value {
					array.push(v);
				}
			}
		}

		// Insert final key if there is one.
		if let Some(x) = next_key {
			let v = std::mem::replace(&mut next_value, Value::None);
			res.insert(x, v);
		}

		Value::Object(Object::from(res))
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		// For each kv pair in the object:
		//  - If the value is an array, insert each value into the header map.
		//  - Otherwise, insert the value into the header map.
		let Value::Object(Object(o)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		let mut res = http::HeaderMap::new();
		for (k, v) in o.into_iter() {
			let k = k.parse::<http::HeaderName>()?;
			match v {
				Value::Array(Array(a)) => {
					for v in a {
						let v = v.into_string()?.parse()?;
						res.insert(k.clone(), v);
					}
				}
				Value::String(v) => {
					res.insert(k, v.parse()?);
				}
				unexpected => {
					return Err(conversion_error(Self::kind_of(), unexpected));
				}
			}
		}
		Ok(res)
	}
}

impl SurrealValue for http::StatusCode {
	fn kind_of() -> Kind {
		Kind::Number
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::Number(_))
	}

	fn into_value(self) -> Value {
		Value::Number(Number::Int(self.as_u16() as i64))
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		let Value::Number(Number::Int(n)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(http::StatusCode::from_u16(n as u16).unwrap())
	}
}
