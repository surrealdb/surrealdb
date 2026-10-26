/**
[test]
run = false
reason = "Setup for pgvector_native_*: tables defined in SurrealQL whose columns hold vectors (an array of floats of a given length) and arrays of floats of no length"
*/

DEFINE TABLE pgv_native SCHEMAFULL;
DEFINE FIELD embedding ON pgv_native TYPE option<array<float, 3>>;
DEFINE FIELD label ON pgv_native TYPE option<string>;

DEFINE TABLE pgv_loose SCHEMAFULL;
DEFINE FIELD embedding ON pgv_loose TYPE option<array<float>>;
