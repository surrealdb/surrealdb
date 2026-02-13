//! Random functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

// No argument functions
define_pure_function!(Rand, "rand", () -> Float, crate::fnc::rand::rand);
define_pure_function!(RandBool, "rand::bool", () -> Bool, crate::fnc::rand::bool);
define_pure_function!(RandUlid, "rand::ulid", () -> String, crate::fnc::rand::ulid);
define_pure_function!(RandUuid, "rand::uuid", () -> Uuid, crate::fnc::rand::uuid);
define_pure_function!(RandUuidV4, "rand::uuid::v4", () -> Uuid, crate::fnc::rand::uuid::v4);
define_pure_function!(RandUuidV7, "rand::uuid::v7", () -> Uuid, crate::fnc::rand::uuid::v7);

// Functions with optional arguments - use variadic for flexibility
define_pure_function!(RandDuration, "rand::duration", (?min: Duration, ?max: Duration) -> Duration, crate::fnc::rand::duration);
define_pure_function!(RandFloat, "rand::float", (?min: Float, ?max: Float) -> Float, crate::fnc::rand::float);
define_pure_function!(RandId, "rand::id", (?length: Int, ?charset: String) -> String, crate::fnc::rand::id);
define_pure_function!(RandInt, "rand::int", (?min: Int, ?max: Int) -> Int, crate::fnc::rand::int);
define_pure_function!(RandString, "rand::string", (?length: Int, ?charset: String) -> String, crate::fnc::rand::string);
define_pure_function!(RandTime, "rand::time", (?min: Datetime, ?max: Datetime) -> Datetime, crate::fnc::rand::time);

// Variadic function
define_pure_function!(RandEnum, "rand::enum", (...values: Any) -> Any, crate::fnc::rand::r#enum);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		Rand,
		RandBool,
		RandDuration,
		RandEnum,
		RandFloat,
		RandId,
		RandInt,
		RandString,
		RandTime,
		RandUlid,
		RandUuid,
		RandUuidV4,
		RandUuidV7,
	);
}
