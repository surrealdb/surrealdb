/**
[env]
namespace = true
database = true

[test]
reason = """
Filtering precedes paging, so a pushed-down predicate is judged whether or not
any row survives `LIMIT` or `START`. A cast, a permission or a function in it
is an evaluation the statement owes, and the statement reports it.

The dynamic scan judges its rows before it pages them: the array branch reads
the source rows even under `LIMIT 0`, and the scalar branch judges its one row
under `LIMIT 0` or any `START`. The array branch needs no early exit — the
truncation it applies after the predicate leaves nothing.

An array holding more than one row is judged whole, not up to the first row a
zero limit could stop at: the last two queries count the judgements with a
sequence, and the one before them fails on a row that is not the first.

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
