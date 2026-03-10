/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Tests that cyclic dependencies among computed fields are detected at DEFINE time"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> b -> a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
error = "Cyclic dependency detected among computed fields: a -> d -> b -> a"

*/

-- Self-referential computed field: a depends on itself
DEFINE TABLE tb1 SCHEMAFULL;
DEFINE FIELD a ON tb1 COMPUTED a * 2;

-- Mutual cycle: a depends on b, b depends on a
DEFINE TABLE tb2 SCHEMAFULL;
DEFINE FIELD a ON tb2 COMPUTED b + 1;
DEFINE FIELD b ON tb2 COMPUTED a + 1;

-- Non-cyclic chain: a -> b -> c (should succeed)
DEFINE TABLE tb3 SCHEMAFULL;
DEFINE FIELD c ON tb3 COMPUTED 42;
DEFINE FIELD b ON tb3 COMPUTED c + 1;
DEFINE FIELD a ON tb3 COMPUTED b + c;

-- Deeper cycle: Start with a valid compute tree.
{
    DEFINE TABLE tb4 SCHEMAFULL;
    DEFINE FIELD a ON tb4 TYPE number DEFAULT 1;
    DEFINE FIELD b ON tb4 COMPUTED a + 1;
    DEFINE FIELD c ON tb4 COMPUTED a + b;
    DEFINE FIELD d ON tb4 COMPUTED c * b;
    RETURN "OK";
};
-- This is now a cycle: a -> d -> b -> a
DEFINE FIELD OVERWRITE a ON tb4 COMPUTED d * 2;
