use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use anyhow::Context;
use rust_decimal::Decimal;

use crate as surrealdb_types;
use crate::error::{ConversionError, length_mismatch_error, out_of_range_error};
use crate::{
	Array, Bytes, Datetime, Duration, File, Geometry, Kind, Number, Object, Range, RecordId, Set,
	SurrealNone, SurrealNull, Table, Uuid, Value, kind,
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
	fn is_value(value: &Value) -> bool {
		value.is_kind(&Self::kind_of())
	}
	/// Converts this type into a SurrealDB value
	fn into_value(self) -> Value;
	/// Attempts to convert a SurrealDB value into this type
	fn from_value(value: Value) -> anyhow::Result<Self>
	where
		Self: Sized;
}

impl<T: SurrealValue> SurrealValue for Box<T> {
	fn kind_of() -> Kind {
		T::kind_of()
	}

	fn is_value(value: &Value) -> bool {
		T::is_value(value)
	}

	fn into_value(self) -> Value {
		T::into_value(*self)
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		T::from_value(value).map(Box::new)
	}
}

impl<T: SurrealValue + Clone> SurrealValue for Arc<T> {
	fn kind_of() -> Kind {
		T::kind_of()
	}

	fn is_value(value: &Value) -> bool {
		T::is_value(value)
	}

	fn into_value(self) -> Value {
		T::into_value(self.as_ref().clone())
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		T::from_value(value).map(Arc::new)
	}
}

impl<T: SurrealValue + Clone> SurrealValue for Rc<T> {
	fn kind_of() -> Kind {
		T::kind_of()
	}

	fn is_value(value: &Value) -> bool {
		T::is_value(value)
	}

	fn into_value(self) -> Value {
		T::into_value(self.as_ref().clone())
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		T::from_value(value).map(Rc::new)
	}
}

macro_rules! impl_surreal_value {
	// Generic types
    (
        <$($generic:ident),*> $type:ty as $kind:expr,
        $($is_fnc:ident<$($is_generic:ident),*>)? ($value_is:ident) => $is:expr,
        $($from_fnc:ident<$($from_generic:ident),*>)? ($self:ident) => $into:expr,
        $($into_fnc:ident<$($into_generic:ident),*>)? ($value_into:ident) => $from:expr
		$(, as_ty => $as_fnc:ident<$($as_generic:ident),*> ($self_as:ident) => $as_expr:expr)?
		$(, is_ty_and => $is_and_fnc:ident<$($is_and_generic:ident),*> ($self_is_and:ident, $is_and_callback:ident) => $is_and:expr)?
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
				/// Returns a reference to the inner value if it matches the expected type.
				///
				/// Returns `Some(&T)` if the value is of the expected type, `None` otherwise.
				pub fn $as_fnc<$($as_generic: SurrealValue),*>(&$self_as) -> Option<&$type> {
					$as_expr
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
        $($is_fnc:ident),* ($value_is:ident) => $is:expr,
        $($from_fnc:ident),* ($self:ident) => $into:expr,
        $($into_fnc:ident),* ($value_into:ident) => $from:expr
		$(, as_ty => $($as_fnc:ident),+ ($self_as:ident) => $as_expr:expr)?
		$(, is_ty_and => $($is_and_fnc:ident),+($self_is_and:ident, $is_and_callback:ident) => $is_and:expr)?
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
        )*

        $(
            impl Value {
                /// Converts the given value into a `Value`
                pub fn $from_fnc(value: $type) -> Value {
                    <$type>::into_value(value)
                }
            }
        )*

        $(
            impl Value {
                /// Attempts to convert a SurrealDB value into the given type
                ///
                /// Returns `Ok(T)` if the conversion is successful, `Err(anyhow::Error)` otherwise.
                pub fn $into_fnc(self) -> anyhow::Result<$type> {
                    <$type>::from_value(self)
                }
            }
        )*

		$(
			impl Value {
				$(
					/// Returns a reference to the inner value if it matches the expected type.
					///
					/// Returns `Some(&T)` if the value is of the expected type, `None` otherwise.
					pub fn $as_fnc(&$self_as) -> Option<&$type> {
						$as_expr
					}
				)+
			}
		)?

		$(
			impl Value {
				$(
					/// Checks if this value can be converted to the given type
					///
					/// Returns `true` if the value can be converted to the type, `false` otherwise.
					pub fn $is_and_fnc(&$self_is_and, $is_and_callback: impl FnOnce(&$type) -> bool) -> bool {
						$is_and
					}
				)+
			}
		)?
    };
}

// Concrete type implementations using the macro
impl_surreal_value!(
	Value as kind!(any),
	(_value) => true,
	(self) => self,
	(value) => Ok(value)
);

impl_surreal_value!(
	() as kind!(none),
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
	SurrealNone as kind!(none),
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
	SurrealNull as kind!(null),
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
	bool as kind!(bool),
	is_bool(value) => matches!(value, Value::Bool(_)),
	from_bool(self) => Value::Bool(self),
	into_bool(value) => {
		let Value::Bool(b) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(b)
	},
	as_ty => as_bool(self) => {
		if let Value::Bool(b) = self {
			Some(b)
		} else {
			None
		}
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
	Number as kind!(number),
	is_number(value) => matches!(value, Value::Number(_)),
	from_number(self) => Value::Number(self),
	into_number(value) => {
		let Value::Number(n) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(n)
	},
	as_ty => as_number(self) => {
		if let Value::Number(n) = self {
			Some(n)
		} else {
			None
		}
	},
	is_ty_and => is_number_and(self, callback) => {
		if let Value::Number(n) = self {
			callback(n)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	i64 as kind!(int),
	is_int, is_i64(value) => matches!(value, Value::Number(Number::Int(_))),
	from_int, from_i64(self) => Value::Number(Number::Int(self)),
	into_int, into_i64(value) => {
		let Value::Number(Number::Int(n)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(n)
	},
	as_ty => as_int, as_i64(self) => {
		if let Value::Number(Number::Int(n)) = self {
			Some(n)
		} else {
			None
		}
	},
	is_ty_and => is_int_and, is_i64_and(self, callback) => {
		if let Value::Number(Number::Int(n)) = self {
			callback(n)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	f64 as kind!(float),
	is_float, is_f64(value) => matches!(value, Value::Number(Number::Float(_))),
	from_float, from_f64(self) => Value::Number(Number::Float(self)),
	into_float, into_f64(value) => {
		let Value::Number(Number::Float(n)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(n)
	},
	as_ty => as_float, as_f64(self) => {
		if let Value::Number(Number::Float(n)) = self {
			Some(n)
		} else {
			None
		}
	},
	is_ty_and => is_float_and, is_f64_and(self, callback) => {
		if let Value::Number(Number::Float(n)) = self {
			callback(n)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Decimal as kind!(decimal),
	is_decimal(value) => matches!(value, Value::Number(Number::Decimal(_))),
	from_decimal(self) => Value::Number(Number::Decimal(self)),
	into_decimal(value) => {
		let Value::Number(Number::Decimal(n)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(n)
	},
	as_ty => as_decimal(self) => {
		if let Value::Number(Number::Decimal(n)) = self {
			Some(n)
		} else {
			None
		}
	},
	is_ty_and => is_decimal_and(self, callback) => {
		if let Value::Number(Number::Decimal(n)) = self {
			callback(n)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	String as kind!(string),
	is_string(value) => matches!(value, Value::String(_)),
	from_string(self) => Value::String(self),
	into_string(value) => {
		let Value::String(s) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(s)
	},
	as_ty => as_string(self) => {
		if let Value::String(s) = self {
			Some(s)
		} else {
			None
		}
	},
	is_ty_and => is_string_and(self, callback) => {
		if let Value::String(s) = self {
			callback(s)
		} else {
			false
		}
	}
);

impl SurrealValue for Cow<'static, str> {
	fn kind_of() -> Kind {
		kind!(string)
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
		kind!(string)
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::String(_))
	}

	fn into_value(self) -> Value {
		Value::String(self.to_string())
	}

	fn from_value(_value: Value) -> anyhow::Result<Self> {
		Err(anyhow::anyhow!(
			"Cannot deserialize &'static str from value: static string references cannot be created from runtime values"
		))
	}
}

impl_surreal_value!(
	Duration as kind!(duration),
	is_duration(value) => matches!(value, Value::Duration(_)),
	from_duration(self) => Value::Duration(self),
	into_duration(value) => {
		let Value::Duration(d) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(d)
	},
	as_ty => as_duration(self) => {
		if let Value::Duration(d) = self {
			Some(d)
		} else {
			None
		}
	},
	is_ty_and => is_duration_and(self, callback) => {
		if let Value::Duration(d) = self {
			callback(d)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	std::time::Duration as kind!(duration),
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
	Datetime as kind!(datetime),
	is_datetime(value) => matches!(value, Value::Datetime(_)),
	from_datetime(self) => Value::Datetime(self),
	into_datetime(value) => {
		let Value::Datetime(d) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(d)
	},
	as_ty => as_datetime(self) => {
		if let Value::Datetime(d) = self {
			Some(d)
		} else {
			None
		}
	},
	is_ty_and => is_datetime_and(self, callback) => {
		if let Value::Datetime(d) = self {
			callback(d)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	chrono::DateTime<chrono::Utc> as kind!(datetime),
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
	Uuid as kind!(uuid),
	is_uuid(value) => matches!(value, Value::Uuid(_)),
	from_uuid(self) => Value::Uuid(self),
	into_uuid(value) => {
		let Value::Uuid(u) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(u)
	},
	as_ty => as_uuid(self) => {
		if let Value::Uuid(u) = self {
			Some(u)
		} else {
			None
		}
	},
	is_ty_and => is_uuid_and(self, callback) => {
		if let Value::Uuid(u) = self {
			callback(u)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	uuid::Uuid as kind!(uuid),
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
	Array as kind!(array),
	is_array(value) => matches!(value, Value::Array(_)),
	from_array(self) => Value::Array(self),
	into_array(value) => {
		let Value::Array(a) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(a)
	},
	as_ty => as_array(self) => {
		if let Value::Array(a) = self {
			Some(a)
		} else {
			None
		}
	},
	is_ty_and => is_array_and(self, callback) => {
		if let Value::Array(a) = self {
			callback(a)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Set as kind!(set),
	is_set(value) => matches!(value, Value::Set(_)),
	from_set(self) => Value::Set(self),
	into_set(value) => {
		let Value::Set(s) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(s)
	},
	as_ty => as_set(self) => {
		if let Value::Set(s) = self {
			Some(s)
		} else {
			None
		}
	},
	is_ty_and => is_set_and(self, callback) => {
		if let Value::Set(s) = self {
			callback(s)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Object as kind!(object),
	is_object(value) => matches!(value, Value::Object(_)),
	from_object(self) => Value::Object(self),
	into_object(value) => {
		let Value::Object(o) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(o)
	},
	as_ty => as_object(self) => {
		if let Value::Object(o) = self {
			Some(o)
		} else {
			None
		}
	},
	is_ty_and => is_object_and(self, callback) => {
		if let Value::Object(o) = self {
			callback(o)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Geometry as kind!(geometry),
	is_geometry(value) => matches!(value, Value::Geometry(_)),
	from_geometry(self) => Value::Geometry(self),
	into_geometry(value) => {
		let Value::Geometry(g) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(g)
	},
	as_ty => as_geometry(self) => {
		if let Value::Geometry(g) = self {
			Some(g)
		} else {
			None
		}
	},
	is_ty_and => is_geometry_and(self, callback) => {
		if let Value::Geometry(g) = self {
			callback(g)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Bytes as kind!(bytes),
	is_bytes(value) => matches!(value, Value::Bytes(_)),
	from_bytes(self) => Value::Bytes(self),
	into_bytes(value) => {
		let Value::Bytes(b) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(b)
	},
	as_ty => as_bytes(self) => {
		if let Value::Bytes(b) = self {
			Some(b)
		} else {
			None
		}
	},
	is_ty_and => is_bytes_and(self, callback) => {
		if let Value::Bytes(b) = self {
			callback(b)
		} else {
			false
		}
	}
);

// NOTE: Vec<u8> is intentionally NOT implemented to force explicit usage of Bytes.
// This prevents ambiguity: should Vec<u8> be bytes or an array of numbers?
// Users should explicitly use:
//   - Bytes(vec) for binary data
//   - Vec<i64> or similar for arrays of numbers

impl_surreal_value!(
	bytes::Bytes as kind!(bytes),
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
	Table as kind!(table),
	is_table(value) => {
		matches!(value, Value::Table(_))
	},
	from_table(self) => Value::Table(self),
	into_table(value) => {
		match value {
			Value::Table(t) => Ok(t),
			_ => Err(conversion_error(Self::kind_of(), value)),
		}
	},
	as_ty => as_table(self) => {
		if let Value::Table(t) = self {
			Some(t)
		} else {
			None
		}
	},
	is_ty_and => is_table_and(self, callback) => {
		if let Value::Table(t) = self {
			callback(t)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	RecordId as kind!(record),
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
				let first = a.first().context("Invalid record id: array is empty")?;
				first.clone().into_record()
			}
			_ => Err(conversion_error(Self::kind_of(), value)),
		}
	},
	as_ty => as_record(self) => {
		if let Value::RecordId(r) = self {
			Some(r)
		} else {
			None
		}
	},
	is_ty_and => is_record_and(self, callback) => {
		if let Value::RecordId(r) = self {
			callback(r)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	File as kind!(file),
	is_file(value) => matches!(value, Value::File(_)),
	from_file(self) => Value::File(self),
	into_file(value) => {
		let Value::File(f) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(f)
	},
	as_ty => as_file(self) => {
		if let Value::File(f) = self {
			Some(f)
		} else {
			None
		}
	},
	is_ty_and => is_file_and(self, callback) => {
		if let Value::File(f) = self {
			callback(f)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	Range as kind!(range),
	is_range(value) => matches!(value, Value::Range(_)),
	from_range(self) => Value::Range(Box::new(self)),
	into_range(value) => {
		let Value::Range(r) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(*r)
	},
	as_ty => as_range(self) => {
		if let Value::Range(r) = self {
			Some(r)
		} else {
			None
		}
	},
	is_ty_and => is_range_and(self, callback) => {
		if let Value::Range(r) = self {
			callback(r)
		} else {
			false
		}
	}
);

// Generic implementations using the macro
impl_surreal_value!(
	<T> Vec<T> as kind!(array<(T::kind_of())>),
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
	<T> Option<T> as kind!(none | (T::kind_of())),
	is_option<T>(value) => matches!(value, Value::None) || T::is_value(value),
	from_option<T>(self) => self.map(T::into_value).unwrap_or(Value::None),
	into_option<T>(value) => match value{
		Value::None => Ok(None),
		x => T::from_value(x).map(Some).map_err(|e| anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e)),
	}
);

impl_surreal_value!(
	<V> BTreeMap<String, V> as kind!(object),
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
	<V> HashMap<String, V> as kind!(object),
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
	geo::Point as kind!(geometry<point>),
	is_point(value) => matches!(value, Value::Geometry(Geometry::Point(_))),
	from_point(self) => Value::Geometry(Geometry::Point(self)),
	into_point(value) => {
		let Value::Geometry(Geometry::Point(p)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(p)
	},
	as_ty => as_point(self) => {
		if let Value::Geometry(Geometry::Point(p)) = self {
			Some(p)
		} else {
			None
		}
	},
	is_ty_and => is_point_and(self, callback) => {
		if let Value::Geometry(Geometry::Point(p)) = self {
			callback(p)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::LineString as kind!(geometry<line>),
	is_line(value) => matches!(value, Value::Geometry(Geometry::Line(_))),
	from_line(self) => Value::Geometry(Geometry::Line(self)),
	into_line(value) => {
		let Value::Geometry(Geometry::Line(l)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(l)
	},
	as_ty => as_line(self) => {
		if let Value::Geometry(Geometry::Line(l)) = self {
			Some(l)
		} else {
			None
		}
	},
	is_ty_and => is_line_and(self, callback) => {
		if let Value::Geometry(Geometry::Line(l)) = self {
			callback(l)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::Polygon as kind!(geometry<polygon>),
	is_polygon(value) => matches!(value, Value::Geometry(Geometry::Polygon(_))),
	from_polygon(self) => Value::Geometry(Geometry::Polygon(self)),
	into_polygon(value) => {
		let Value::Geometry(Geometry::Polygon(p)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(p)
	},
	as_ty => as_polygon(self) => {
		if let Value::Geometry(Geometry::Polygon(p)) = self {
			Some(p)
		} else {
			None
		}
	},
	is_ty_and => is_polygon_and(self, callback) => {
		if let Value::Geometry(Geometry::Polygon(p)) = self {
			callback(p)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::MultiPoint as kind!(geometry<multipoint>),
	is_multipoint(value) => matches!(value, Value::Geometry(Geometry::MultiPoint(_))),
	from_multipoint(self) => Value::Geometry(Geometry::MultiPoint(self)),
	into_multipoint(value) => {
		let Value::Geometry(Geometry::MultiPoint(m)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(m)
	},
	as_ty => as_multipoint(self) => {
		if let Value::Geometry(Geometry::MultiPoint(m)) = self {
			Some(m)
		} else {
			None
		}
	},
	is_ty_and => is_multipoint_and(self, callback) => {
		if let Value::Geometry(Geometry::MultiPoint(m)) = self {
			callback(m)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::MultiLineString as kind!(geometry<multiline>),
	is_multiline(value) => matches!(value, Value::Geometry(Geometry::MultiLine(_))),
	from_multiline(self) => Value::Geometry(Geometry::MultiLine(self)),
	into_multiline(value) => {
		let Value::Geometry(Geometry::MultiLine(m)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(m)
	},
	as_ty => as_multiline(self) => {
		if let Value::Geometry(Geometry::MultiLine(m)) = self {
			Some(m)
		} else {
			None
		}
	},
	is_ty_and => is_multiline_and(self, callback) => {
		if let Value::Geometry(Geometry::MultiLine(m)) = self {
			callback(m)
		} else {
			false
		}
	}
);

impl_surreal_value!(
	geo::MultiPolygon as kind!(geometry<multipolygon>),
	is_multipolygon(value) => matches!(value, Value::Geometry(Geometry::MultiPolygon(_))),
	from_multipolygon(self) => Value::Geometry(Geometry::MultiPolygon(self)),
	into_multipolygon(value) => {
		let Value::Geometry(Geometry::MultiPolygon(m)) = value else {
			return Err(conversion_error(Self::kind_of(), value));
		};
		Ok(m)
	},
	as_ty => as_multipolygon(self) => {
		if let Value::Geometry(Geometry::MultiPolygon(m)) = self {
			Some(m)
		} else {
			None
		}
	},
	is_ty_and => is_multipolygon_and(self, callback) => {
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
					kind!([$(($t::kind_of())),+])
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
                        return Err(length_mismatch_error($n, a.len(), std::any::type_name::<Self>()));
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
	ConversionError::from_value(expected, &got).into()
}

/// Non-standard numeric implementations, such as u8, u16, u32, u64, i8, i16, i32, isize, f32
macro_rules! impl_numeric {
	// Integer types that need checked conversion from i64
	(int: $($k:ty => ($is_fn:ident, $from_fn:ident, $into_fn:ident, $is_and_fn:ident, $as_fn:ident)),+ $(,)?) => {
		$(
			impl SurrealValue for $k {
				fn kind_of() -> Kind {
					kind!(number)
				}

				fn is_value(value: &Value) -> bool {
					matches!(value, Value::Number(_))
				}

				fn into_value(self) -> Value {
					Value::Number(Number::Int(self as i64))
				}

				fn from_value(value: Value) -> anyhow::Result<Self> {
					let Value::Number(Number::Int(n)) = value else {
						return Err(conversion_error(Self::kind_of(), value));
					};
					<$k>::try_from(n)
						.map_err(|_| out_of_range_error(n, std::any::type_name::<$k>()))
				}
			}

			impl Value {
				/// Checks if this value can be converted to the given type
				pub fn $is_fn(&self) -> bool {
					<$k>::is_value(self)
				}

				/// Converts the given value into a `Value`
				pub fn $from_fn(value: $k) -> Value {
					<$k>::into_value(value)
				}

				/// Attempts to convert a SurrealDB value into the given type
				pub fn $into_fn(self) -> anyhow::Result<$k> {
					<$k>::from_value(self)
				}

				/// Checks if this value matches the type and passes the callback check
				pub fn $is_and_fn(&self, callback: impl FnOnce(&$k) -> bool) -> bool {
					if let Value::Number(Number::Int(n)) = self {
						if let Ok(v) = <$k>::try_from(*n) {
							return callback(&v);
						}
					}
					false
				}

				/// Returns a reference-like value if it matches the expected type
				/// Note: For integer types, this returns `None` since the value needs conversion
				pub fn $as_fn(&self) -> Option<$k> {
					if let Value::Number(Number::Int(n)) = self {
						<$k>::try_from(*n).ok()
					} else {
						None
					}
				}
			}
		)+
	};

	// Float types
	(float: $($k:ty => ($is_fn:ident, $from_fn:ident, $into_fn:ident, $is_and_fn:ident, $as_fn:ident)),+ $(,)?) => {
		$(
			impl SurrealValue for $k {
				fn kind_of() -> Kind {
					kind!(number)
				}

				fn is_value(value: &Value) -> bool {
					matches!(value, Value::Number(_))
				}

				fn into_value(self) -> Value {
					Value::Number(Number::Float(self as f64))
				}

				fn from_value(value: Value) -> anyhow::Result<Self> {
					let Value::Number(Number::Float(n)) = value else {
						return Err(conversion_error(Self::kind_of(), value));
					};
					Ok(n as $k)
				}
			}

			impl Value {
				/// Checks if this value can be converted to the given type
				pub fn $is_fn(&self) -> bool {
					<$k>::is_value(self)
				}

				/// Converts the given value into a `Value`
				pub fn $from_fn(value: $k) -> Value {
					<$k>::into_value(value)
				}

				/// Attempts to convert a SurrealDB value into the given type
				pub fn $into_fn(self) -> anyhow::Result<$k> {
					<$k>::from_value(self)
				}

				/// Checks if this value matches the type and passes the callback check
				pub fn $is_and_fn(&self, callback: impl FnOnce(&$k) -> bool) -> bool {
					if let Value::Number(Number::Float(n)) = self {
						let v = *n as $k;
						callback(&v)
					} else {
						false
					}
				}

				/// Returns the converted value if it matches the expected type
				/// Note: For float types, this returns a converted value, not a reference
				pub fn $as_fn(&self) -> Option<$k> {
					if let Value::Number(Number::Float(n)) = self {
						Some(*n as $k)
					} else {
						None
					}
				}
			}
		)+
	};
}

// Non-primary integer types with conversion
impl_numeric! {
	int:
		i8 => (is_i8, from_i8, into_i8, is_i8_and, as_i8),
		i16 => (is_i16, from_i16, into_i16, is_i16_and, as_i16),
		i32 => (is_i32, from_i32, into_i32, is_i32_and, as_i32),
		// i64 is implemented with the impl_surreal_value! macro
		isize => (is_isize, from_isize, into_isize, is_isize_and, as_isize),
		u8 => (is_u8, from_u8, into_u8, is_u8_and, as_u8),
		u16 => (is_u16, from_u16, into_u16, is_u16_and, as_u16),
		u32 => (is_u32, from_u32, into_u32, is_u32_and, as_u32),
		u64 => (is_u64, from_u64, into_u64, is_u64_and, as_u64),
		usize => (is_usize, from_usize, into_usize, is_usize_and, as_usize),
}

// Non-primary float types with conversion
impl_numeric! {
	float:
		f32 => (is_f32, from_f32, into_f32, is_f32_and, as_f32),
		// f64 is implemented with the impl_surreal_value! macro
}

impl SurrealValue for serde_json::Value {
	fn kind_of() -> Kind {
		kind!(any)
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
					kind!(array<(T::kind_of()), $n>)
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
						return Err(length_mismatch_error($n, a.len(), std::any::type_name::<Self>()));
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
		kind!(object)
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
		kind!(number)
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
		http::StatusCode::from_u16(n as u16).context("Failed to convert status code")
	}
}
