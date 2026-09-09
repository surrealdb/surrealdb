/**
[env]
planner-strategy = ["compute-only"]

[env.capabilities]
deny-functions = ["string::len", "search::score", "type::field"]

[test]
reason = """
Half of a pair. Same queries as
`denied_function_physical_expr_streaming.surql`, pinned to the legacy compute
path; the two files' expected results are the divergence.

A denied function must not execute whichever syntax reaches it. This side is
the intended contract: method syntax resolves to the same registry entry as
the direct call and is refused, an index function bound to a MATCHES predicate
is refused, and a projection function is refused outside projection position.

Three receivers on the streaming side reach the function without a capability
check and so return a value here where this file errors: method dispatch,
`IndexFunctionExec` and `ProjectionFunctionExec`.

`SELECT type::field('a') FROM t` is the exception both engines agree on: it is
allowed in projection position on either path, so gating it on one side only
would create a divergence in the other direction.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: t:1, s: 'abcd' }]"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
error = "Function 'string::len' is not allowed to be executed"

[[test.results]]
error = "Function 'type::field' is not allowed to be executed"

[[test.results]]
value = "[{ a: 5 }]"

[[test.results]]
error = "Function 'type::field' is not allowed to be executed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'search::score' is not allowed to be executed"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 5, s = 'abcd';

-- Control: the denied function called directly, which every path refuses.
RETURN string::len('abcd');
SELECT string::len(s) AS n FROM t;

-- Method syntax reaches the same registry entry, so it carries the same
-- denial. Both a bare receiver and a field receiver, because the streaming
-- planner builds a different expression for each.
RETURN 'abcd'.len();
SELECT s.len() AS n FROM t;

-- A projection function outside projection position is an ordinary call and
-- is denied; in projection position it is allowed on both paths.
RETURN type::field('a');
SELECT type::field('a') FROM t;
SELECT * FROM t WHERE type::field('a') = 5;

-- An index function is bound to the MATCHES predicate at plan time. The
-- binding does not exempt it from the capability check.
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX t_s ON t FIELDS s FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
SELECT id, search::score(1) AS score FROM t WHERE s @1@ 'abcd';
