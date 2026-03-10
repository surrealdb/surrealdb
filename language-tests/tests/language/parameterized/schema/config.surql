/**
[env]
namespace = "test"

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ database: 'test_db', namespace: 'test_ns' }"""

*/
LET $namespace = "test_ns";
LET $database = "test_db";
LET $user_role = "admin";

-- Test DEFINE CONFIG DEFAULT with parameterized NAMESPACE and DATABASE values
DEFINE CONFIG DEFAULT NAMESPACE $namespace DATABASE $database;

(INFO FOR KV).defaults;
