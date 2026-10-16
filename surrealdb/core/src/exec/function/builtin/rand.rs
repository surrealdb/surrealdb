//! Random functions

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

// Every function here takes no execution context. Each is defined with
// `define_pure_function!` and called synchronously. Their results are not
// deterministic, so each declares `EffectSet::ENTROPY`. That stops the
// literal folder from reducing a call to one constant shared by the whole
// statement.

// No argument functions
define_pure_function!(Rand, "rand", effects: EffectSet::ENTROPY, fallibility: Fallibility::INFALLIBLE, () -> Float, crate::fnc::rand::rand);
define_pure_function!(RandBool, "rand::bool", effects: EffectSet::ENTROPY, fallibility: Fallibility::INFALLIBLE, () -> Bool, crate::fnc::rand::bool);
define_pure_function!(RandUuidV4, "rand::uuid::v4", effects: EffectSet::ENTROPY, fallibility: Fallibility::INFALLIBLE, () -> Uuid, crate::fnc::rand::uuid::v4);

// Optional timestamp argument functions
define_pure_function!(RandUlid, "rand::ulid", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (?timestamp: Datetime) -> String, crate::fnc::rand::ulid);
define_pure_function!(RandUuid, "rand::uuid", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (?timestamp: Datetime) -> Uuid, crate::fnc::rand::uuid);
define_pure_function!(RandUuidV7, "rand::uuid::v7", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (?timestamp: Datetime) -> Uuid, crate::fnc::rand::uuid::v7);

// Functions with optional or range arguments
define_pure_function!(RandDuration, "rand::duration", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (min: Duration, max: Duration) -> Duration, crate::fnc::rand::duration);
define_pure_function!(RandFloat, "rand::float", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (?min: Float, ?max: Float) -> Float, crate::fnc::rand::float);
define_pure_function!(RandId, "rand::id", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (?length: Int, ?max: Int) -> String, crate::fnc::rand::id);
define_pure_function!(RandInt, "rand::int", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (?min: Int, ?max: Int) -> Int, crate::fnc::rand::int);
define_pure_function!(RandString, "rand::string", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (?length: Int, ?max: Int) -> String, crate::fnc::rand::string);
define_pure_function!(RandTime, "rand::time", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (?min: Any, ?max: Any) -> Datetime, crate::fnc::rand::time);

// Variadic function
define_pure_function!(RandEnum, "rand::enum", effects: EffectSet::ENTROPY, fallibility: Fallibility::VALUE, (...values: Any) -> Any, crate::fnc::rand::r#enum);

/// The `rand::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&Rand,
	&RandBool,
	&RandDuration,
	&RandEnum,
	&RandFloat,
	&RandId,
	&RandInt,
	&RandString,
	&RandTime,
	&RandUlid,
	&RandUuid,
	&RandUuidV4,
	&RandUuidV7,
];
