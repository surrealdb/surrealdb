//! Type conversion and checking functions

use anyhow::Result;

use crate::exec::ContextLevel;
use crate::exec::function::{FunctionRegistry, ProjectionFunction, Signature};
use crate::exec::parts::array_ops::evaluate_all;
use crate::exec::parts::field::evaluate_field;
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::fnc::args::FromArgs;
use crate::val::Value;
use crate::{define_pure_function, register_functions, syn};

// Type conversion functions
define_pure_function!(TypeArray, "type::array", effects: EffectSet::PURE, (value: Any) -> Any, crate::fnc::r#type::array);
define_pure_function!(TypeBool, "type::bool", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::bool);
define_pure_function!(TypeBytes, "type::bytes", effects: EffectSet::PURE, (value: Any) -> Any, crate::fnc::r#type::bytes);
define_pure_function!(TypeDatetime, "type::datetime", effects: EffectSet::PURE, (value: Any) -> Datetime, crate::fnc::r#type::datetime);
define_pure_function!(TypeDecimal, "type::decimal", effects: EffectSet::PURE, (value: Any) -> Decimal, crate::fnc::r#type::decimal);
define_pure_function!(TypeDuration, "type::duration", effects: EffectSet::PURE, (value: Any) -> Duration, crate::fnc::r#type::duration);
define_pure_function!(TypeFile, "type::file", effects: EffectSet::PURE, (bucket: String, key: String) -> Any, crate::fnc::r#type::file);
define_pure_function!(TypeFloat, "type::float", effects: EffectSet::PURE, (value: Any) -> Float, crate::fnc::r#type::float);
define_pure_function!(TypeGeometry, "type::geometry", effects: EffectSet::PURE, (value: Any) -> Any, crate::fnc::r#type::geometry);
define_pure_function!(TypeInt, "type::int", effects: EffectSet::PURE, (value: Any) -> Int, crate::fnc::r#type::int);
define_pure_function!(TypeNumber, "type::number", effects: EffectSet::PURE, (value: Any) -> Number, crate::fnc::r#type::number);
define_pure_function!(TypeOf, "type::of", effects: EffectSet::PURE, (value: Any) -> String, crate::fnc::r#type::type_of);
define_pure_function!(TypePoint, "type::point", effects: EffectSet::PURE, (value: Any) -> Any, crate::fnc::r#type::point);
define_pure_function!(TypeRange, "type::range", effects: EffectSet::PURE, (value: Any) -> Any, crate::fnc::r#type::range);
define_pure_function!(TypeRecord, "type::record", effects: EffectSet::PURE, (value: Any, ?table: Any) -> Any, crate::fnc::r#type::record);
define_pure_function!(TypeSet, "type::set", effects: EffectSet::PURE, (value: Any) -> Any, crate::fnc::r#type::set);
define_pure_function!(TypeString, "type::string", effects: EffectSet::PURE, (value: Any) -> String, crate::fnc::r#type::string);
define_pure_function!(TypeStringLossy, "type::string_lossy", effects: EffectSet::PURE, (value: Any) -> String, crate::fnc::r#type::string_lossy);
define_pure_function!(TypeTable, "type::table", effects: EffectSet::PURE, (value: Any) -> Any, crate::fnc::r#type::table);
define_pure_function!(TypeUuid, "type::uuid", effects: EffectSet::PURE, (value: Any) -> Uuid, crate::fnc::r#type::uuid);

// Type checking functions
define_pure_function!(TypeIsArray, "type::is_array", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::array);
define_pure_function!(TypeIsBool, "type::is_bool", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::bool);
define_pure_function!(TypeIsBytes, "type::is_bytes", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::bytes);
define_pure_function!(TypeIsCollection, "type::is_collection", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::collection);
define_pure_function!(TypeIsDatetime, "type::is_datetime", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::datetime);
define_pure_function!(TypeIsDecimal, "type::is_decimal", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::decimal);
define_pure_function!(TypeIsDuration, "type::is_duration", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::duration);
define_pure_function!(TypeIsFloat, "type::is_float", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::float);
define_pure_function!(TypeIsGeometry, "type::is_geometry", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::geometry);
define_pure_function!(TypeIsInt, "type::is_int", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::int);
define_pure_function!(TypeIsLine, "type::is_line", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::line);
define_pure_function!(TypeIsMultiline, "type::is_multiline", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::multiline);
define_pure_function!(TypeIsMultipoint, "type::is_multipoint", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::multipoint);
define_pure_function!(TypeIsMultipolygon, "type::is_multipolygon", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::multipolygon);
define_pure_function!(TypeIsNone, "type::is_none", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::none);
define_pure_function!(TypeIsNull, "type::is_null", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::null);
define_pure_function!(TypeIsNumber, "type::is_number", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::number);
define_pure_function!(TypeIsObject, "type::is_object", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::object);
define_pure_function!(TypeIsPoint, "type::is_point", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::point);
define_pure_function!(TypeIsPolygon, "type::is_polygon", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::polygon);
define_pure_function!(TypeIsRange, "type::is_range", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::range);
define_pure_function!(TypeIsRecord, "type::is_record", effects: EffectSet::PURE, (value: Any, ?table: String) -> Bool, crate::fnc::r#type::is::record);
define_pure_function!(TypeIsSet, "type::is_set", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::set);
define_pure_function!(TypeIsString, "type::is_string", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::string);
define_pure_function!(TypeIsUuid, "type::is_uuid", effects: EffectSet::PURE, (value: Any) -> Bool, crate::fnc::r#type::is::uuid);

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
			Part::First => match &current {
				Value::Array(arr) => arr.first().cloned().unwrap_or(Value::None),
				_ => Value::None,
			},
			Part::Last => match &current {
				Value::Array(arr) => arr.last().cloned().unwrap_or(Value::None),
				_ => Value::None,
			},
			_ => current.pick(std::slice::from_ref(part)),
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
				.map_err(|e| anyhow::anyhow!("Invalid field path '{}': {}", path, e))?
				.into();

			let value = if let Some(current) = ctx.current_value {
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
					.map_err(|e| anyhow::anyhow!("Invalid field path '{}': {}", path, e))?
					.into();

				let value = if let Some(current) = ctx.current_value {
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

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		TypeArray,
		TypeBool,
		TypeBytes,
		TypeDatetime,
		TypeDecimal,
		TypeDuration,
		TypeFile,
		TypeFloat,
		TypeGeometry,
		TypeInt,
		TypeIsArray,
		TypeIsBool,
		TypeIsBytes,
		TypeIsCollection,
		TypeIsDatetime,
		TypeIsDecimal,
		TypeIsDuration,
		TypeIsFloat,
		TypeIsGeometry,
		TypeIsInt,
		TypeIsLine,
		TypeIsMultiline,
		TypeIsMultipoint,
		TypeIsMultipolygon,
		TypeIsNone,
		TypeIsNull,
		TypeIsNumber,
		TypeIsObject,
		TypeIsPoint,
		TypeIsPolygon,
		TypeIsRange,
		TypeIsRecord,
		TypeIsSet,
		TypeIsString,
		TypeIsUuid,
		TypeNumber,
		TypeOf,
		TypePoint,
		TypeRange,
		TypeRecord,
		TypeSet,
		TypeString,
		TypeStringLossy,
		TypeTable,
		TypeUuid,
	);
	// Register projection functions (these produce field bindings, not single values)
	registry.register_projection(TypeField);
	registry.register_projection(TypeFields);
}
