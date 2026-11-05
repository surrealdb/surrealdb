//! Type conversion and checking functions

use anyhow::Result;

use crate::exec::function::{FunctionRegistry, ProjectionFunction, ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::exec::vm::access::array_ops::evaluate_all;
use crate::exec::vm::access::field::evaluate_field;
use crate::exec::{ContextLevel, EvalContext};
use crate::expr::Kind;
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::fnc::args::FromArgs;
use crate::val::Value;
use crate::{define_pure_function, syn};

// Type conversion functions
define_pure_function!(TypeArray, "type::array", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::r#type::array);
define_pure_function!(TypeBool, "type::bool", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Bool, crate::fnc::r#type::bool);
define_pure_function!(TypeBytes, "type::bytes", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::r#type::bytes);
define_pure_function!(TypeDatetime, "type::datetime", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Datetime, crate::fnc::r#type::datetime);
define_pure_function!(TypeDate, "type::date", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Date, crate::fnc::r#type::date);
define_pure_function!(TypeTime, "type::time", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Time, crate::fnc::r#type::time);
define_pure_function!(TypeTimestamp, "type::timestamp", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Timestamp, crate::fnc::r#type::timestamp);
define_pure_function!(TypeInterval, "type::interval", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Interval, crate::fnc::r#type::interval);
define_pure_function!(TypeDecimal, "type::decimal", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Decimal, crate::fnc::r#type::decimal);
define_pure_function!(TypeDuration, "type::duration", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Duration, crate::fnc::r#type::duration);
define_pure_function!(TypeF16, "type::f16", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> F16, crate::fnc::r#type::f16);
define_pure_function!(TypeF32, "type::f32", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> F32, crate::fnc::r#type::f32);
define_pure_function!(TypeFile, "type::file", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (bucket: String, key: String) -> Any, crate::fnc::r#type::file);
define_pure_function!(TypeFloat, "type::float", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Float, crate::fnc::r#type::float);
define_pure_function!(TypeGeometry, "type::geometry", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::r#type::geometry);
define_pure_function!(TypeI16, "type::i16", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> I16, crate::fnc::r#type::i16);
define_pure_function!(TypeI32, "type::i32", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> I32, crate::fnc::r#type::i32);
define_pure_function!(TypeI8, "type::i8", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> I8, crate::fnc::r#type::i8);
define_pure_function!(TypeInt, "type::int", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Int, crate::fnc::r#type::int);
define_pure_function!(TypeNumber, "type::number", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Number, crate::fnc::r#type::number);
define_pure_function!(TypeOf, "type::of", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> String, crate::fnc::r#type::type_of);
define_pure_function!(TypePoint, "type::point", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::r#type::point);
define_pure_function!(TypeRange, "type::range", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::r#type::range);
define_pure_function!(TypeRecord, "type::record", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any, ?table: Any) -> Any, crate::fnc::r#type::record);
define_pure_function!(TypeSet, "type::set", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::r#type::set);
define_pure_function!(TypeString, "type::string", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> String, crate::fnc::r#type::string);
define_pure_function!(TypeStringLossy, "type::string_lossy", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> String, crate::fnc::r#type::string_lossy);
define_pure_function!(TypeTable, "type::table", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::r#type::table);
define_pure_function!(TypeU16, "type::u16", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> U16, crate::fnc::r#type::u16);
define_pure_function!(TypeU32, "type::u32", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> U32, crate::fnc::r#type::u32);
define_pure_function!(TypeU64, "type::u64", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> U64, crate::fnc::r#type::u64);
define_pure_function!(TypeU8, "type::u8", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> U8, crate::fnc::r#type::u8);
define_pure_function!(TypeUuid, "type::uuid", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Uuid, crate::fnc::r#type::uuid);

// Type checking functions
define_pure_function!(TypeIsArray, "type::is_array", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::array);
define_pure_function!(TypeIsBool, "type::is_bool", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::bool);
define_pure_function!(TypeIsBytes, "type::is_bytes", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::bytes);
define_pure_function!(TypeIsCollection, "type::is_collection", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::collection);
define_pure_function!(TypeIsDatetime, "type::is_datetime", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::datetime);
define_pure_function!(TypeIsDate, "type::is_date", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::date);
define_pure_function!(TypeIsTime, "type::is_time", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::time);
define_pure_function!(TypeIsTimestamp, "type::is_timestamp", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::timestamp);
define_pure_function!(TypeIsInterval, "type::is_interval", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::interval);
define_pure_function!(TypeIsDecimal, "type::is_decimal", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::decimal);
define_pure_function!(TypeIsDuration, "type::is_duration", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::duration);
define_pure_function!(TypeIsF16, "type::is_f16", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::f16);
define_pure_function!(TypeIsF32, "type::is_f32", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::f32);
define_pure_function!(TypeIsFloat, "type::is_float", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::float);
define_pure_function!(TypeIsGeometry, "type::is_geometry", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::geometry);
define_pure_function!(TypeIsI16, "type::is_i16", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::i16);
define_pure_function!(TypeIsI32, "type::is_i32", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::i32);
define_pure_function!(TypeIsI8, "type::is_i8", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::i8);
define_pure_function!(TypeIsInt, "type::is_int", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::int);
define_pure_function!(TypeIsLine, "type::is_line", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::line);
define_pure_function!(TypeIsMultiline, "type::is_multiline", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::multiline);
define_pure_function!(TypeIsMultipoint, "type::is_multipoint", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::multipoint);
define_pure_function!(TypeIsMultipolygon, "type::is_multipolygon", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::multipolygon);
define_pure_function!(TypeIsNone, "type::is_none", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::none);
define_pure_function!(TypeIsNull, "type::is_null", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::null);
define_pure_function!(TypeIsNumber, "type::is_number", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::number);
define_pure_function!(TypeIsObject, "type::is_object", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::object);
define_pure_function!(TypeIsPoint, "type::is_point", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::point);
define_pure_function!(TypeIsPolygon, "type::is_polygon", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::polygon);
define_pure_function!(TypeIsRange, "type::is_range", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::range);
define_pure_function!(TypeIsRecord, "type::is_record", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any, ?table: String) -> Bool, crate::fnc::r#type::is::record);
define_pure_function!(TypeIsSet, "type::is_set", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::set);
define_pure_function!(TypeIsString, "type::is_string", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::string);
define_pure_function!(TypeIsU16, "type::is_u16", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::u16);
define_pure_function!(TypeIsU32, "type::is_u32", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::u32);
define_pure_function!(TypeIsU64, "type::is_u64", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::u64);
define_pure_function!(TypeIsU8, "type::is_u8", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::u8);
define_pure_function!(TypeIsUuid, "type::is_uuid", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::r#type::is::uuid);

// =========================================================================
// type::field / type::fields -- shared idiom evaluation helper
// =========================================================================

/// Walk an idiom path on a value with record-link-aware evaluation.
///
/// Unlike `Value::pick()` (which only handles Object and Array), this
/// uses `evaluate_field` for `Part::Field` access, so `RecordId` values
/// are automatically fetched from the database before continuing the path.
async fn evaluate_idiom_on_value(
	value: &Value,
	parts: &[Part],
	ctx: EvalContext<'_>,
) -> Result<Value> {
	let mut current = value.clone();
	for part in parts {
		current = match part {
			Part::Field(f) => evaluate_field(&current, f.as_str(), ctx.clone())
				.await
				.map_err(|cf| anyhow::anyhow!("{cf}"))?,
			Part::All => {
				evaluate_all(&current, ctx.clone()).await.map_err(|cf| anyhow::anyhow!("{cf}"))?
			}
			Part::First => {
				if let Value::Array(arr) = &current {
					arr.first().cloned().unwrap_or(Value::None)
				} else {
					Value::None
				}
			}
			Part::Last => {
				if let Value::Array(arr) = &current {
					arr.last().cloned().unwrap_or(Value::None)
				} else {
					Value::None
				}
			}
			Part::Flatten
			| Part::Where(_)
			| Part::Lookup(_)
			| Part::Value(_)
			| Part::Start(_)
			| Part::Method(..)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(..)
			| Part::Doc
			| Part::RepeatRecurse => current.pick(std::slice::from_ref(part)),
		};
	}
	Ok(current)
}

// =========================================================================
// type::field - Get a field value by string path (Projection Function)
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct TypeField;

impl ProjectionFunction for TypeField {
	fn name(&self) -> &'static str {
		"type::field"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(
			Projection,
			"type::field",
			Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
		)
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("path", Kind::String).returns(Kind::Any)
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Vec<(Idiom, Value)>>> {
		Box::pin(async move {
			let (path,): (String,) = FromArgs::from_args("type::field", args)?;

			let idiom: Idiom = syn::idiom(&path)
				.map_err(|e| anyhow::anyhow!("Invalid field path '{}': {}", path, e))?;

			let value = if let Some(current) = ctx.value_in_scope().read()? {
				evaluate_idiom_on_value(current, &idiom.0, ctx.clone()).await?
			} else {
				Value::None
			};

			Ok(vec![(idiom, value)])
		})
	}
}

// =========================================================================
// type::fields - Get multiple field values by string paths (Projection Function)
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct TypeFields;

impl ProjectionFunction for TypeFields {
	fn name(&self) -> &'static str {
		"type::fields"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(
			Projection,
			"type::fields",
			Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
		)
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("paths", Kind::Array(Box::new(Kind::String), None)).returns(Kind::Any)
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Vec<(Idiom, Value)>>> {
		Box::pin(async move {
			let (paths,): (Vec<String>,) = FromArgs::from_args("type::fields", args)?;

			let mut results = Vec::with_capacity(paths.len());

			for path in paths {
				let idiom: Idiom = syn::idiom(&path)
					.map_err(|e| anyhow::anyhow!("Invalid field path '{}': {}", path, e))?;

				let value = if let Some(current) = ctx.value_in_scope().read()? {
					evaluate_idiom_on_value(current, &idiom.0, ctx.clone()).await?
				} else {
					Value::None
				};

				results.push((idiom, value));
			}

			Ok(results)
		})
	}
}

/// The `type::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&TypeArray,
	&TypeBool,
	&TypeBytes,
	&TypeDate,
	&TypeDatetime,
	&TypeInterval,
	&TypeTime,
	&TypeTimestamp,
	&TypeDecimal,
	&TypeDuration,
	&TypeF16,
	&TypeF32,
	&TypeFile,
	&TypeFloat,
	&TypeGeometry,
	&TypeI16,
	&TypeI32,
	&TypeI8,
	&TypeInt,
	&TypeIsArray,
	&TypeIsBool,
	&TypeIsBytes,
	&TypeIsCollection,
	&TypeIsDate,
	&TypeIsDatetime,
	&TypeIsInterval,
	&TypeIsTime,
	&TypeIsTimestamp,
	&TypeIsDecimal,
	&TypeIsDuration,
	&TypeIsF16,
	&TypeIsF32,
	&TypeIsFloat,
	&TypeIsGeometry,
	&TypeIsI16,
	&TypeIsI32,
	&TypeIsI8,
	&TypeIsInt,
	&TypeIsLine,
	&TypeIsMultiline,
	&TypeIsMultipoint,
	&TypeIsMultipolygon,
	&TypeIsNone,
	&TypeIsNull,
	&TypeIsNumber,
	&TypeIsObject,
	&TypeIsPoint,
	&TypeIsPolygon,
	&TypeIsRange,
	&TypeIsRecord,
	&TypeIsSet,
	&TypeIsString,
	&TypeIsU16,
	&TypeIsU32,
	&TypeIsU64,
	&TypeIsU8,
	&TypeIsUuid,
	&TypeNumber,
	&TypeOf,
	&TypePoint,
	&TypeRange,
	&TypeRecord,
	&TypeSet,
	&TypeString,
	&TypeStringLossy,
	&TypeTable,
	&TypeU16,
	&TypeU32,
	&TypeU64,
	&TypeU8,
	&TypeUuid,
];

pub fn register(registry: &mut FunctionRegistry) {
	// Register projection functions (these produce field bindings, not single values)
	registry.register_projection(TypeField);
	registry.register_projection(TypeFields);
}
