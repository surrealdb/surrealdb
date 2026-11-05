/**
[env]
namespace = true
database = true

[test]
reason = "Issue #7408, the runtime-string funnel: an index column that is computed at runtime is resolved by re-parsing the computed string as an idiom. That funnel read the string as a general expression, so a reserved keyword such as `function` failed with the same syntax error the issue reports, while `type::field()` on the same statement resolved it correctly."
issue = 7408

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { i_kw: 'DEFINE INDEX i_kw ON dy FIELDS `function`', i_kw2: 'DEFINE INDEX i_kw2 ON dy FIELDS `select`', i_ok: 'DEFINE INDEX i_ok ON dy FIELDS okname', i_tf: 'DEFINE INDEX i_tf ON dy FIELDS `function`' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { i_kw: 'DEFINE INDEX i_kw ON dy FIELDS `function`', i_kw2: 'DEFINE INDEX i_kw2 ON dy FIELDS `select`', i_nested: 'DEFINE INDEX i_nested ON dy FIELDS `function`.sub', i_ok: 'DEFINE INDEX i_ok ON dy FIELDS okname', i_tail: 'DEFINE INDEX i_tail ON dy FIELDS meta.function', i_tf: 'DEFINE INDEX i_tf ON dy FIELDS `function`' }, lives: {  }, tables: {  } }"

*/

DEFINE TABLE dy SCHEMALESS;

-- A non-keyword name always worked, so it is the control.
LET $ok = 'okname';
DEFINE INDEX i_ok ON dy FIELDS $ok;

-- The reported failure: the computed name is a reserved keyword. Rendered bare,
-- it has to be read as a field name and not as the start of a call.
LET $kw = 'function';
DEFINE INDEX i_kw ON dy FIELDS $kw;

-- `select` takes the other expression-parse failure path.
LET $kw2 = 'select';
DEFINE INDEX i_kw2 ON dy FIELDS $kw2;

-- type::field() already resolved keywords correctly, so it pins the behaviour
-- the param form must now agree with.
DEFINE INDEX i_tf ON dy FIELDS type::field('function');

INFO FOR TABLE dy;

-- A computed path with a keyword at the head, and one with a keyword after the
-- dot, must both resolve.
LET $nested = 'function.sub';
DEFINE INDEX i_nested ON dy FIELDS $nested;
LET $tail = 'meta.function';
DEFINE INDEX i_tail ON dy FIELDS $tail;
INFO FOR TABLE dy;
