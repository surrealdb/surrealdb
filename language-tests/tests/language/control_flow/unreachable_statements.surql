/**
[env]
namespace = true
database = true

[test]
reason = """
A statement after a RETURN, THROW or BREAK in the same block never runs and is
not planned: a refusal that depends on the catalog or the session, such as a
MATCHES no index answers, is not raised, and a schema statement in it defines
nothing. A LET no statement may hold — of a protected name, or written as an
expression — is refused from the text written, wherever it stands, so it is
the block's error even there.
"""

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "1"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "NONE"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
error = "An error occurred: x"

[[test.results]]
value = "1"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: {  }, users: {  } }"
*/
{ RETURN 1; LET $auth = 2 };
{ RETURN 1; SELECT * FROM t WHERE a @@ 'x' };
{ RETURN 1; SELECT * FROM t WHERE (LET $x = 1) };
{ THROW 'a'; LET $session = 1 };
FOR $x IN [1] { BREAK; LET $auth = 1 };
FOR $x IN [1] { CONTINUE; SELECT * FROM t WHERE a @@ 'x' };
{ DEFINE TABLE later; RETURN 1; LET $auth = 1 };
{ IF true { RETURN 1 } ELSE { RETURN 2 }; SELECT * FROM t WHERE a @@ 'x' };
{ IF $c { RETURN 1 } ELSE { RETURN 2 }; SELECT * FROM t WHERE a @@ 'x' };
{ IF true { THROW 'x' }; SELECT * FROM t WHERE a @@ 'x' };
{ RETURN 1; DEFINE TABLE dead };
INFO FOR DB;
