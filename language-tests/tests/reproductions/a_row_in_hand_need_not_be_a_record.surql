/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Reading a field off a record id fetches that record, deriving its
	`COMPUTED` fields afresh, so a rewrite that leaves two evaluations where
	the statement wrote one can report two different values. `$this`, `$self`
	and `$parent` are no exemption from that: they name the row in hand, but a
	row is not always a record — `SELECT VALUE link FROM o` yields bare record
	ids — and a second read off one fetches again.

	`fn::pair($x) { [$x, $x] }` is the shape that exposes it. Inlining
	substitutes the argument for every mention of the parameter, so an
	argument it may duplicate has to answer the same both times; the call
	itself evaluates its argument once, and the two elements are equal.

	The `COMPUTED` field draws from a sequence rather than at random so that a
	second fetch is always visible: two reads of one record can never answer
	alike. The last two queries are the controls — a field base, which was
	never exempt, and a row that really is a record.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/

DEFINE SEQUENCE s BATCH 1000 START 0;
DEFINE TABLE p SCHEMALESS;
DEFINE FIELD n ON p COMPUTED sequence::nextval('s');
DEFINE TABLE o SCHEMALESS;
CREATE p:1 RETURN NONE;
CREATE o:1 SET link = p:1 RETURN NONE;
DEFINE FUNCTION fn::pair($x: any) { RETURN [$x, $x]; };

-- The row in hand is a bare record id, so `$this.n` fetches.
LET $r = (SELECT VALUE fn::pair($this.n) FROM (SELECT VALUE link FROM o))[0];
RETURN $r[0] = $r[1];

-- Control: a field base, which was never read as a row in hand.
LET $t = (SELECT VALUE fn::pair(link.n) FROM o)[0];
RETURN $t[0] = $t[1];

-- Control: a row that is a record, where the read is of the row itself.
LET $u = (SELECT VALUE fn::pair($this.link.n) FROM o)[0];
RETURN $u[0] = $u[1];
