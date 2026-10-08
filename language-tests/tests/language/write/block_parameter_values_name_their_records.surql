/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A parameter a block binds holds values, and a write naming it classifies
	each value as a target, as the same value written in place would be: rows
	a subquery returned name their records, which the write reads from storage
	and writes whole, so a field the rows left out keeps its stored value and a
	DELETE returns the record as it was stored; a table is scanned; a record
	id names its record; any other value is refused.
"""

[[test.results]]
value = "[{ balance: 200, id: acct:2, name: 'y' }]"

[[test.results]]
value = "[{ balance: 200, id: acct:2, name: 'y', touched: true }]"

[[test.results]]
value = "[{ balance: 200, id: acct:2, name: 'y', touched: true }]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[true]"

[[test.results]]
error = "Cannot execute UPDATE statement using value: 'nope'"

[[test.results]]
value = "[{ again: true, balance: 200, id: acct:2, name: 'y', third: true, touched: true }]"

[[test.results]]
value = "[]"

*/

CREATE acct:2 SET name = 'y', balance = 200;
RETURN { LET $rows = (SELECT id, name FROM acct); UPDATE $rows SET touched = true; };
SELECT * FROM acct;
RETURN { LET $t = type::table('acct'); UPDATE $t SET again = true RETURN VALUE again; };
RETURN { LET $id = acct:2; UPDATE $id SET third = true RETURN VALUE third; };
RETURN { LET $v = 'nope'; UPDATE $v SET x = 1; };
RETURN { LET $rows = (SELECT id FROM acct); DELETE $rows RETURN BEFORE; };
SELECT * FROM acct;
