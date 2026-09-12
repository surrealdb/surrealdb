//! Random functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

// Every function here needs no execution context, so it is defined with
// `define_pure_function!` and invoked synchronously. None of them are
// deterministic, though, so each declares `EffectSet::ENTROPY`, which keeps
// the planner's literal folder from collapsing a call to a single constant
// reused for the whole statement.

// No argument functions
define_pure_function!(Rand, "rand", effects: EffectSet::ENTROPY, () -> Float, crate::fnc::rand::rand);
define_pure_function!(RandBool, "rand::bool", effects: EffectSet::ENTROPY, () -> Bool, crate::fnc::rand::bool);
define_pure_function!(RandUuidV4, "rand::uuid::v4", effects: EffectSet::ENTROPY, () -> Uuid, crate::fnc::rand::uuid::v4);

// Optional timestamp argument functions
define_pure_function!(RandUlid, "rand::ulid", effects: EffectSet::ENTROPY, (?timestamp: Datetime) -> String, crate::fnc::rand::ulid);
define_pure_function!(RandUuid, "rand::uuid", effects: EffectSet::ENTROPY, (?timestamp: Datetime) -> Uuid, crate::fnc::rand::uuid);
define_pure_function!(RandUuidV7, "rand::uuid::v7", effects: EffectSet::ENTROPY, (?timestamp: Datetime) -> Uuid, crate::fnc::rand::uuid::v7);

// Functions with optional or range arguments
define_pure_function!(RandDuration, "rand::duration", effects: EffectSet::ENTROPY, (min: Duration, max: Duration) -> Duration, crate::fnc::rand::duration);
define_pure_function!(RandFloat, "rand::float", effects: EffectSet::ENTROPY, (?min: Float, ?max: Float) -> Float, crate::fnc::rand::float);
define_pure_function!(RandId, "rand::id", effects: EffectSet::ENTROPY, (?length: Int, ?max: Int) -> String, crate::fnc::rand::id);
define_pure_function!(RandInt, "rand::int", effects: EffectSet::ENTROPY, (?min: Int, ?max: Int) -> Int, crate::fnc::rand::int);
define_pure_function!(RandString, "rand::string", effects: EffectSet::ENTROPY, (?length: Int, ?max: Int) -> String, crate::fnc::rand::string);
define_pure_function!(RandTime, "rand::time", effects: EffectSet::ENTROPY, (?min: Any, ?max: Any) -> Datetime, crate::fnc::rand::time);

// Variadic function
define_pure_function!(RandEnum, "rand::enum", effects: EffectSet::ENTROPY, (...values: Any) -> Any, crate::fnc::rand::r#enum);

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
