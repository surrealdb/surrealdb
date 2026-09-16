/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	One column with effects is free to run a column at a time — there is no
	second effect for it to be reordered against — but only while the columns
	around it are total. A column that raises decides where the statement
	stops, and a column-at-a-time pass reaches that stop at a different point
	in the effectful column's rows than the written order does.

	Here the raising column is written first. In written order the first row
	casts, dereferences and draws; the second row fails its cast and ends the
	statement, so exactly one draw happened. A column at a time would cast
	both rows, end the statement on the second before the dereferencing column
	ran at all, and leave the sequence untouched. The draw count after the
	failed statement is the difference.

	`a_dereference_runs_a_body_once_per_row` pins the other side of the same
	rule: two columns that both dereference stay in written order whatever
	else they do.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'no'`"

[[test.results]]
value = "1"
*/

DEFINE SEQUENCE s BATCH 1000 START 0;

DEFINE TABLE p SCHEMALESS;
DEFINE FIELD n ON p COMPUTED sequence::nextval('s');
CREATE p:1 RETURN NONE;

-- The raising column stands before the dereferencing one, so the row it
-- raises on decides how many rows the dereference reached.
SELECT <int> x AS b, link.n AS a FROM [{ x: "1", link: p:1 }, { x: "no", link: p:1 }];

-- One draw happened: the first row's, made before the second row raised.
RETURN sequence::nextval('s');
