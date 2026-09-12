/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A closure body carries nothing of the transaction that created it. The
	closure value is invoked by later statements, so a `SELECT` inside it
	reads the catalog of the invoking statement: a `COMPUTED` field defined
	after the `LET` appears in the rows the closure returns, a field removed
	after the `LET` is gone from them, an index removed after the `LET` is not
	read, a function redefined after the `LET` is called in its new form, and
	a field guard defined after the `LET` stands over the closure's read. Both
	engines agree on every strategy.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 2, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, tier: 'gold' }]"

[[test.results]]
value = "[{ id: u:2, tier: 'bronze' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, tier: 'gold' }, { id: u:2, tier: 'bronze' }]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = 1;
LET $f = || (SELECT * FROM t);
DEFINE FIELD b ON t COMPUTED a + 1;
RETURN $f();
REMOVE FIELD b ON t;
RETURN $f();
DEFINE TABLE u SCHEMALESS;
CREATE u:1 SET tier = 'gold';
CREATE u:2 SET tier = 'bronze';
DEFINE INDEX idx_tier ON u FIELDS tier;
LET $g = || (SELECT id FROM u WHERE tier = 'gold');
REMOVE INDEX idx_tier ON u;
RETURN $g();
DEFINE FUNCTION fn::v() { RETURN 1; };
LET $h = || fn::v();
DEFINE FUNCTION OVERWRITE fn::v() { RETURN 2; };
RETURN $h();
LET $i = || (SELECT * FROM u ORDER BY id);
DEFINE FIELD tier ON u TYPE string PERMISSIONS FOR select WHERE tier = 'gold';
RETURN $i();
