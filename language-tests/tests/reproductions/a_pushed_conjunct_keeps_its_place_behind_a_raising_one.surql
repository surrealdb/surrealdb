/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	`AND` reaches its conjuncts left to right, so a conjunct written after one
	that can raise is judged only on the rows that one admitted. The rule that
	moves conjuncts beneath a computed-field layer partitions the predicate by
	what each term reads, which puts a moved term's judgement before the terms
	written ahead of it — and where one of those can raise, the row the moved
	term rejects never reaches it, so the statement reports no rows where it
	owes an error.

	A rewrite may reorder an evaluation and may not remove one. Moving a term
	across a *total* conjunct reorders; moving it across a raising one removes
	the raising conjunct's evaluation for every row the moved term refuses,
	which is why the rule stops at the first raising term it retains.

	The four shapes pin both directions of that rule. The first is the one that
	diverged: the raising conjunct is written first, so nothing may move past
	it. The second has the raising conjunct written last, where `a = 1` legally
	refuses the row before it — the pushdown this rule exists for is unchanged,
	and asserting it here is what keeps the fix from being "retain everything".
	The third makes the row survive, so the retained conjunct still runs and
	still raises. The fourth is a plain predicate over the same layer, which
	must still answer rows.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

# The raising conjunct is written first, so `a = 1` may not be judged ahead of
# it. With the term moved beneath the layer the row is refused there and this
# answers `[]`.
[[test.results]]
error = "An error occurred: written_first"

# Written last, `a = 1` refuses the row before the raising term is reached —
# by the statement's own order, not by a rewrite. The term still moves.
[[test.results]]
value = "[]"

# The row survives the moved term, so the retained conjunct is reached and
# raises. Without this the test would pass on a rule that never moved
# anything and never evaluated the layer column.
[[test.results]]
error = "An error occurred: row_survives"

# A predicate with nothing to order still pushes and still answers.
[[test.results]]
value = "[0]"
*/

DEFINE TABLE u SCHEMALESS;
DEFINE FIELD c ON u COMPUTED 0;
CREATE u:1 SET a = 0 RETURN NONE;

SELECT * FROM u WHERE (IF c = 0 THEN THROW 'written_first' ELSE true END) AND a = 1;
SELECT * FROM u WHERE a = 1 AND (IF c = 0 THEN THROW 'written_last' ELSE true END);
SELECT * FROM u WHERE a = 0 AND (IF c = 0 THEN THROW 'row_survives' ELSE true END);
SELECT VALUE a FROM u WHERE a = 0;
