/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A view's definition reads every table its clauses name, not only the tables in
its `FROM`: here a subquery reads `ka`. Defined before `ka` exists, the
definition fails and the view is not created; defined after `ka` holds its
record, it computes its rows from it. This is the order an export has to keep
when it writes a view's definition: after the tables the view's subqueries read,
records included, as well as after its `FROM` tables.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "The table 'ka' does not exist"

[[test.results]]
value = "{ kb: 'DEFINE TABLE kb TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ksub:1, pv: 1 }]"
*/

DEFINE TABLE kb SCHEMAFULL;
DEFINE FIELD v ON kb TYPE int;
CREATE kb:1 SET v = 2 RETURN NONE;
DEFINE TABLE ksub AS SELECT id, (SELECT VALUE v FROM ONLY ka:1) AS pv FROM kb;
(INFO FOR DB).tables;
DEFINE TABLE ka SCHEMAFULL;
DEFINE FIELD v ON ka TYPE int;
CREATE ka:1 SET v = 1 RETURN NONE;
DEFINE TABLE ksub AS SELECT id, (SELECT VALUE v FROM ONLY ka:1) AS pv FROM kb;
SELECT * FROM ksub;
