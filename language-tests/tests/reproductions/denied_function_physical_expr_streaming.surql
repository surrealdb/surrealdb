/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[env.capabilities]
deny-functions = ["string::len", "search::score", "type::field"]

[test]
reason = """
Half of a pair. Same queries as
`denied_function_physical_expr_legacy.surql`, pinned to the streaming
planner; the two files' expected results are the divergence.

Three streaming physical expressions invoke a function straight from the
registry without calling `check_allowed_function`, so a denied function
executes and returns a value where the legacy path errors:

- method dispatch (`MethodPart`) — `'abcd'.len()` returns 4, and
  `s.len()` projects it, though `string::len(s)` is refused in the same
  statement position. `MethodPart` gates experimental targets only.
- `ProjectionFunctionExec` — `type::field` outside projection position is
  reached: as a bare `RETURN` it yields NONE rather than erroring, and in a
  WHERE it evaluates and admits the row.
- `IndexFunctionExec` — `search::score` bound to the MATCHES predicate is
  invoked and scores the row.

The legacy expectations are the intended contract; these are the values the
gap currently produces. Both files change together when it closes.

`BuiltinFunctionExec` does check, which is why the two direct calls error here
and pin the deny list as genuinely in force — without them a passing file
could just mean the capability never applied.
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
value = "4"

[[test.results]]
value = "[{ n: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5 }]"

[[test.results]]
value = "[{ a: 5, id: t:1, s: 'abcd' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, score: 0f }]"
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
