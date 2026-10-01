/**
[env.capabilities]
deny-functions = ["string::len", "search::score", "type::field"]

[test]
reason = """
A denied function must not execute whichever syntax reaches it. Method syntax
resolves to the same registry entry as the direct call and is refused; a
function bound to a MATCHES predicate at plan time is refused, the binding
being no exemption; and a projection function outside projection position is an
ordinary call and is refused.

`SELECT type::field('a') FROM t` is the one exception, on every engine: in
projection position the call names a field of the output shape rather than
computing a value, and it is allowed whatever the deny list says.

The two direct calls pin the deny list as genuinely in force — without them a
passing file could just mean the capability never applied.
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
-- is denied; in projection position it is allowed on every engine.
RETURN type::field('a');
SELECT type::field('a') FROM t;
SELECT * FROM t WHERE type::field('a') = 5;

-- An index function is bound to the MATCHES predicate at plan time. The
-- binding does not exempt it from the capability check.
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX t_s ON t FIELDS s FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
SELECT id, search::score(1) AS score FROM t WHERE s @1@ 'abcd';
