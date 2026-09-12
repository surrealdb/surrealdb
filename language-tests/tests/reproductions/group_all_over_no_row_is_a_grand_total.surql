/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	`GROUP ALL` names one group over the whole input, and that group belongs to
	the statement rather than to the data: it is opened whether or not a row
	reaches it, so a query over nothing answers with the grand total of
	nothing rather than with no row at all. What each aggregate reports there
	is its own answer over no values, the same answer its scalar form gives
	for an empty array — which is why `math::max` reports `-Infinity` and
	`math::mean` `NaN` rather than a row-shaped absence, and why `time::min`
	and `time::max` report `NONE`, having no such bound to fall back on. Each
	scalar form is checked alongside its aggregate so the two cannot drift
	apart in value. They are not checked for numeric *type*: a total is a
	float on the per-row engine and an integer on the streaming one, here as
	for a group with rows, and `group_aggregate_numeric_type_*` is where
	that is pinned.

	A column that is not an aggregate follows the same rule: one the row
	cannot influence has the statement's value — evaluated once for the group,
	against no row, whether or not that value is fixed — and one that reads the
	row reports what it collected from no rows.

	Nothing about the input may change the answer: an empty table, an empty
	array, a range of record ids, a record id naming a row that is not there,
	a predicate no row satisfies and an unset parameter all report the same
	total, and so does a source the count-by-key shortcut declines. The
	shortcut decides which plan runs, never what the plan answers.

	`GROUP BY` is the other half of the rule and the control here: a group key
	can only be learned from a row, so an input that yields none leaves no
	group and the statement reports nothing.

	`SELECT *, count() ... GROUP BY` is deliberately absent: both engines
	reject it, differing only in that the streaming message carries an
	`Invalid query: ` prefix, which is a message difference rather than a
	behavioural one.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ s: 0 }]"

[[test.results]]
value = "0"

[[test.results]]
value = "[{ count: 0, s: 0 }]"

[[test.results]]
value = "[{ m: -Infinity }]"

[[test.results]]
value = "-Infinity"

[[test.results]]
value = "[{ m: Infinity }]"

[[test.results]]
value = "Infinity"

[[test.results]]
value = "[{ a: NaN }]"

[[test.results]]
value = "NaN"

[[test.results]]
value = "[{ hi: NONE, lo: NONE }]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[{ g: [] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 0, collected: [], p: 7, r: 3, t: 'lit' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[0]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ id: e:5, k: 5 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ s: 5 }]"

[[test.results]]
value = "[{ count: 1, k: 5 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: 0, total: 0 }]"
*/

DEFINE TABLE e SCHEMALESS;

-- An empty table, and the key sets a range of record ids names.
SELECT count() FROM e GROUP ALL;
SELECT count() FROM e:1..9 GROUP ALL;
SELECT count() FROM e:1.. GROUP ALL;

-- Every aggregate reports its answer over no values, which is the answer its
-- scalar form gives for no values.
SELECT math::sum(k) AS s FROM e GROUP ALL;
RETURN math::sum([]);
SELECT count(), math::sum(k) AS s FROM e GROUP ALL;
SELECT math::max(k) AS m FROM [] GROUP ALL;
RETURN math::max([]);
SELECT math::min(k) AS m FROM [] GROUP ALL;
RETURN math::min([]);
SELECT math::mean(k) AS a FROM [] GROUP ALL;
RETURN math::mean([]);
SELECT time::min(d) AS lo, time::max(d) AS hi FROM [] GROUP ALL;
RETURN [time::min([]), time::max([])];
SELECT array::group(k) AS g FROM [] GROUP ALL;

-- A column the row cannot influence has the statement's value — including one
-- whose value is not fixed, which is evaluated once for the group rather than
-- skipped; one that reads the row reports what it collected from no rows.
LET $p = 7;
SELECT $p AS p, 'lit' AS t, rand::int(3, 3) AS r, k AS collected, count() AS c
FROM e GROUP ALL;

-- Control: a GROUP BY key comes from a row, so no row means no group.
SELECT k, count() FROM [] GROUP BY k;

-- The source's shape does not matter, and neither does a VALUE projection.
SELECT count() FROM [] GROUP ALL;
SELECT VALUE count() FROM [] GROUP ALL;
SELECT count() FROM $unset_param GROUP ALL;

-- Control: no `GROUP ALL`, so nothing asks for a total.
SELECT count() FROM e;

-- A record id naming a row that is not there counts nothing; an object is one
-- row, so it counts as one.
SELECT count() FROM e:1 GROUP ALL;
SELECT count() FROM {} GROUP ALL;

-- With rows to count, every shape reports them.
CREATE e:5 SET k = 5;
SELECT count() FROM e GROUP ALL;
SELECT count() FROM e:5 GROUP ALL;
SELECT count() FROM e:6 GROUP ALL;
SELECT math::sum(k) AS s FROM e GROUP ALL;
SELECT k, count() FROM e GROUP BY k;

-- A predicate that no row satisfies still has its group.
SELECT count() FROM e WHERE k = 99 GROUP ALL;

-- A declared column is no different from an undeclared one: the total is the
-- aggregate's answer over no values, not a value of the column's type.
DEFINE TABLE typed SCHEMAFULL;
DEFINE FIELD c ON typed TYPE string;
DEFINE FIELD v ON typed TYPE int;
SELECT count() AS n, math::sum(v) AS total FROM typed GROUP ALL;
