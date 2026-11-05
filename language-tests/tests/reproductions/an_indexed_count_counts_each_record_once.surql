/**
[env]
namespace = true
database = true

[test]
reason = """
An index over a field lists an array value once per element, so a record
holding `[3, 4]` is listed under `3`. `[3, 4] = 3` is false, so
`count() ... WHERE age = 3` counts that record not at all: the correct count
below is 1, the answer `WITH NOINDEX` gives. An indexed count counts index keys
only for an index whose columns are declared a kind that holds no array, and
otherwise counts the records the predicate admits.

The typed table's column holds no array, so its keys are counted.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"
*/

{
	DEFINE INDEX by_age ON t FIELDS age;
	CREATE t:1 SET age = 3 RETURN NONE;
	CREATE t:2 SET age = [3, 4] RETURN NONE;
	CREATE t:3 SET age = 4 RETURN NONE;
	DEFINE FIELD n ON u TYPE int;
	DEFINE INDEX by_n ON u FIELDS n;
	CREATE u:1 SET n = 3 RETURN NONE;
	CREATE u:2 SET n = 3 RETURN NONE;
	CREATE u:3 SET n = 4 RETURN NONE;
};

SELECT count() FROM t WHERE age = 3 GROUP ALL;
SELECT count() FROM t WITH NOINDEX WHERE age = 3 GROUP ALL;
SELECT count() FROM u WHERE n = 3 GROUP ALL;
