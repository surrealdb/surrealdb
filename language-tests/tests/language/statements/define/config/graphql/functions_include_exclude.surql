/**
[test]
reason = "Test DEFINE CONFIG GRAPHQL FUNCTIONS INCLUDE and FUNCTIONS EXCLUDE, including `fn::ns::name` references and round-trip via INFO FOR DB."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES NONE FUNCTIONS INCLUDE fn::greet, fn::math::add' }"

[[test.results]]
value = "[{ graphql: { functions: { include: ['greet', 'math::add'] }, tables: NONE } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ GraphQL: 'GRAPHQL TABLES NONE FUNCTIONS EXCLUDE fn::greet' }"

[[test.results]]
value = "[{ graphql: { functions: { exclude: ['greet'] }, tables: NONE } }]"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/

-- 0-2: Define some functions to reference in INCLUDE/EXCLUDE (including a nested-namespace one)
DEFINE FUNCTION fn::greet($name: string) { RETURN 'hi ' + $name; };
DEFINE FUNCTION fn::math::add($a: int, $b: int) { RETURN $a + $b; };
DEFINE FUNCTION fn::math::sub($a: int, $b: int) { RETURN $a - $b; };

-- 3: FUNCTIONS INCLUDE accepts both `fn::name` and `fn::ns::name`
DEFINE CONFIG GRAPHQL FUNCTIONS INCLUDE fn::greet, fn::math::add;

-- 4: INFO FOR DB round-trips the clause exactly as written
(INFO FOR DB).configs;

-- 5: STRUCTURE form stores names without the `fn::` prefix (matches catalog storage)
(INFO FOR DB STRUCTURE).configs;

-- 6: FUNCTIONS EXCLUDE is symmetric
DEFINE CONFIG OVERWRITE GRAPHQL FUNCTIONS EXCLUDE fn::greet;

-- 7: INFO FOR DB round-trips EXCLUDE
(INFO FOR DB).configs;

-- 8: STRUCTURE form for EXCLUDE
(INFO FOR DB STRUCTURE).configs;
