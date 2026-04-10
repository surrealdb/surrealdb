/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "UNIQUE index on a field must stay consistent when UPDATE cycles that field through a value, NONE, NULL, NONE, and back to a value"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ k: 'a', v: 10 }, { k: 'b', v: 20 }, { k: 'c', v: 30 }]"

[[test.results]]
value = "[{ id: t:a, k: 'a' }]"

[[test.results]]
value = "[{ k: 'a', v: NONE }]"

[[test.results]]
value = "[{ id: t:a, k: 'a', v: NULL }]"

[[test.results]]
value = "[{ k: 'a', v: NULL }]"

[[test.results]]
value = "[{ id: t:a, k: 'a' }]"

[[test.results]]
value = "[{ k: 'a', v: NONE }]"

[[test.results]]
value = "[{ id: t:a, k: 'a', v: 15 }]"

[[test.results]]
value = "[{ k: 'a', v: 15 }, { k: 'b', v: 20 }, { k: 'c', v: 30 }]"

*/

{
	DEFINE TABLE OVERWRITE t SCHEMALESS;
	CREATE t:a SET v = 10, k = 'a';
	CREATE t:b SET v = 20, k = 'b';
	CREATE t:c SET v = 30, k = 'c';
	DEFINE INDEX OVERWRITE t_v ON t FIELDS v UNIQUE;
	RETURN "OK";
};

-- Baseline indexed values
RETURN (SELECT k, v FROM t ORDER BY k);
-- Value -> NONE
UPDATE t:a SET v = NONE;
RETURN (SELECT k, v FROM t WHERE v = NONE ORDER BY k);
-- NONE -> NULL
UPDATE t:a SET v = NULL;
RETURN (SELECT k, v FROM t WHERE v = NULL ORDER BY k);
-- NULL -> NONE
UPDATE t:a UNSET v;
RETURN (SELECT k, v FROM t WHERE v = NONE ORDER BY k);
-- NONE -> value again
UPDATE t:a SET v = 15;
RETURN (SELECT k, v FROM t ORDER BY k);
