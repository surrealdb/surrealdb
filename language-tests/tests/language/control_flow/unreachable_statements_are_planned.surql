/**
[env]
namespace = true
database = true

[test]
reason = """
A statement after a RETURN, THROW or BREAK in the same block never runs, but it
is still planned with the block: a refusal its planning raises is the block's
error, and a schema statement in it defines nothing.
"""

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "1"

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "1"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"
*/
{ RETURN 1; LET $auth = 2 };
{ RETURN 1; SELECT * FROM t WHERE a @@ 'x' };
{ THROW 'a'; LET $session = 1 };
FOR $x IN [1] { BREAK; LET $auth = 1 };
{ RETURN 1; DEFINE TABLE dead };
INFO FOR DB;
