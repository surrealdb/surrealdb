/**
[env]
namespace = true
database = true

[test]
reason = """
Filtering precedes paging, so a pushed-down predicate is judged whether or not
any row survives `LIMIT` or `START`. A cast, a permission or a function in it
is an evaluation the statement owes, and the legacy engine reports it.

The dynamic scan took two shortcuts past that: the array branch returned on
`LIMIT 0` before reading the source rows at all, and the scalar branch
returned on `LIMIT 0` or any `START` before judging its one row. Both now
judge first and page after. The array branch needs no early exit — the
truncation it already applies after the predicate leaves nothing.

An array holding more than one row is judged whole, not up to the first row a
zero limit could stop at: the last two queries count the judgements with a
sequence, and the one before them fails on a row that is not the first. Both
engines answer alike.

`array::shuffle` keeps the source out of the constant folder, so the scan is
the thing under test; an invoked closure is the same source with its rows in
the order they were written.
"""

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "3"
*/

SELECT * FROM array::shuffle([{ bad: 'x' }]) WHERE <int> bad = 1 LIMIT 0;
SELECT * FROM array::at(array::shuffle([{ bad: 'x' }]), 0) WHERE <int> bad = 1 START 1;
SELECT * FROM array::at(array::shuffle([{ bad: 'x' }]), 0) WHERE <int> bad = 1 LIMIT 0;
SELECT * FROM array::shuffle([{ bad: 'x' }]) WHERE <int> bad = 1 START 5;

-- A zero limit judges every row of the array, not only the first one it could
-- stop at: the row that fails the cast here is the second.
SELECT * FROM (|| [{ bad: 1 }, { bad: 'x' }])() WHERE <int> bad = 1 LIMIT 0;

-- And the count says so directly.
DEFINE SEQUENCE s BATCH 1000 START 0;
SELECT * FROM (|| [{ a: 1 }, { a: 2 }, { a: 3 }])() WHERE sequence::nextval('s') >= 0 LIMIT 0;
RETURN sequence::nextval('s');
