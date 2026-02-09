//! Type conversion and checking functions

use std::pin::Pin;

use anyhow::Result;

use crate::exec::ContextLevel;
use crate::exec::function::{FunctionRegistry, ProjectionFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::expr::idiom::Idiom;
use crate::fnc::args::FromArgs;
use crate::val::Value;
use crate::{define_pure_function, register_functions, syn};

// Type conversion functions
define_pure_function!(TypeArray, "type::array", (value: Any) -> Any, crate::fnc::r#type::array);
define_pure_function!(TypeBool, "type::bool", (value: Any) -> Bool, crate::fnc::r#type::bool);
define_pure_function!(TypeBytes, "type::bytes", (value: Any) -> Any, crate::fnc::r#type::bytes);
define_pure_function!(TypeDatetime, "type::datetime", (value: Any) -> Datetime, crate::fnc::r#type::datetime);
define_pure_function!(TypeDecimal, "type::decimal", (value: Any) -> Decimal, crate::fnc::r#type::decimal);
define_pure_function!(TypeDuration, "type::duration", (value: Any) -> Duration, crate::fnc::r#type::duration);
define_pure_function!(TypeFile, "type::file", (bucket: String, key: String) -> Any, crate::fnc::r#type::file);
define_pure_function!(TypeFloat, "type::float", (value: Any) -> Float, crate::fnc::r#type::float);
define_pure_function!(TypeGeometry, "type::geometry", (value: Any) -> Any, crate::fnc::r#type::geometry);
define_pure_function!(TypeInt, "type::int", (value: Any) -> Int, crate::fnc::r#type::int);
define_pure_function!(TypeNumber, "type::number", (value: Any) -> Number, crate::fnc::r#type::number);
define_pure_function!(TypeOf, "type::of", (value: Any) -> String, crate::fnc::r#type::type_of);
define_pure_function!(TypePoint, "type::point", (value: Any, ?y: Any) -> Any, crate::fnc::r#type::point);
define_pure_function!(TypeRange, "type::range", (value: Any) -> Any, crate::fnc::r#type::range);
define_pure_function!(TypeRecord, "type::record", (value: Any, ?table: String) -> Any, crate::fnc::r#type::record);
define_pure_function!(TypeSet, "type::set", (value: Any) -> Any, crate::fnc::r#type::set);
define_pure_function!(TypeString, "type::string", (value: Any) -> String, crate::fnc::r#type::string);
define_pure_function!(TypeStringLossy, "type::string_lossy", (value: Any) -> String, crate::fnc::r#type::string_lossy);
define_pure_function!(TypeTable, "type::table", (value: Any) -> Any, crate::fnc::r#type::table);
define_pure_function!(TypeUuid, "type::uuid", (value: Any) -> Uuid, crate::fnc::r#type::uuid);

// Type checking functions
define_pure_function!(TypeIsArray, "type::is_array", (value: Any) -> Bool, crate::fnc::r#type::is::array);
define_pure_function!(TypeIsBool, "type::is_bool", (value: Any) -> Bool, crate::fnc::r#type::is::bool);
define_pure_function!(TypeIsBytes, "type::is_bytes", (value: Any) -> Bool, crate::fnc::r#type::is::bytes);
define_pure_function!(TypeIsCollection, "type::is_collection", (value: Any) -> Bool, crate::fnc::r#type::is::collection);
define_pure_function!(TypeIsDatetime, "type::is_datetime", (value: Any) -> Bool, crate::fnc::r#type::is::datetime);
define_pure_function!(TypeIsDecimal, "type::is_decimal", (value: Any) -> Bool, crate::fnc::r#type::is::decimal);
define_pure_function!(TypeIsDuration, "type::is_duration", (value: Any) -> Bool, crate::fnc::r#type::is::duration);
define_pure_function!(TypeIsFloat, "type::is_float", (value: Any) -> Bool, crate::fnc::r#type::is::float);
define_pure_function!(TypeIsGeometry, "type::is_geometry", (value: Any) -> Bool, crate::fnc::r#type::is::geometry);
define_pure_function!(TypeIsInt, "type::is_int", (value: Any) -> Bool, crate::fnc::r#type::is::int);
define_pure_function!(TypeIsLine, "type::is_line", (value: Any) -> Bool, crate::fnc::r#type::is::line);
define_pure_function!(TypeIsMultiline, "type::is_multiline", (value: Any) -> Bool, crate::fnc::r#type::is::multiline);
define_pure_function!(TypeIsMultipoint, "type::is_multipoint", (value: Any) -> Bool, crate::fnc::r#type::is::multipoint);
define_pure_function!(TypeIsMultipolygon, "type::is_multipolygon", (value: Any) -> Bool, crate::fnc::r#type::is::multipolygon);
define_pure_function!(TypeIsNone, "type::is_none", (value: Any) -> Bool, crate::fnc::r#type::is::none);
define_pure_function!(TypeIsNull, "type::is_null", (value: Any) -> Bool, crate::fnc::r#type::is::null);
define_pure_function!(TypeIsNumber, "type::is_number", (value: Any) -> Bool, crate::fnc::r#type::is::number);
define_pure_function!(TypeIsObject, "type::is_object", (value: Any) -> Bool, crate::fnc::r#type::is::object);
define_pure_function!(TypeIsPoint, "type::is_point", (value: Any) -> Bool, crate::fnc::r#type::is::point);
define_pure_function!(TypeIsPolygon, "type::is_polygon", (value: Any) -> Bool, crate::fnc::r#type::is::polygon);
define_pure_function!(TypeIsRange, "type::is_range", (value: Any) -> Bool, crate::fnc::r#type::is::range);
define_pure_function!(TypeIsRecord, "type::is_record", (value: Any, ?table: Any) -> Bool, crate::fnc::r#type::is::record);
define_pure_function!(TypeIsSet, "type::is_set", (value: Any) -> Bool, crate::fnc::r#type::is::set);
define_pure_function!(TypeIsString, "type::is_string", (value: Any) -> Bool, crate::fnc::r#type::is::string);
define_pure_function!(TypeIsUuid, "type::is_uuid", (value: Any) -> Bool, crate::fnc::r#type::is::uuid);

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
	) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<(Idiom, Value)>>> + Send + 'a>> {
		Box::pin(async move {
			// Extract the string path argument
			let (path,): (String,) = FromArgs::from_args("type::field", args)?;

			// Parse the string as an Idiom
			let idiom: Idiom = syn::idiom(&path)
				.map_err(|e| anyhow::anyhow!("Invalid field path '{}': {}", path, e))?
				.into();

			// Get the field value from the current document
			let value = if let Some(current) = ctx.current_value {
				current.pick(&idiom.0)
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
	) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<(Idiom, Value)>>> + Send + 'a>> {
		Box::pin(async move {
			// Extract the array of string paths
			let (paths,): (Vec<String>,) = FromArgs::from_args("type::fields", args)?;

			let mut results = Vec::with_capacity(paths.len());

			for path in paths {
				// Parse each string as an Idiom
				let idiom: Idiom = syn::idiom(&path)
					.map_err(|e| anyhow::anyhow!("Invalid field path '{}': {}", path, e))?
					.into();

				// Get the field value from the current document
				let value = if let Some(current) = ctx.current_value {
					current.pick(&idiom.0)
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
