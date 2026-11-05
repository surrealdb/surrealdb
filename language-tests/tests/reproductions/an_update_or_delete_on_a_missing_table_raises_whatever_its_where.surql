/**
[env]
namespace = true
database = true

[test]
reason = """
An UPDATE or DELETE on a table that is not defined raises "The table
'nosuch' does not exist", whatever its WHERE: a predicate that is always false
and one that is not, a static target and a dynamic one, with one target or
several, and inside BEGIN ... COMMIT, where the error cancels the transaction.
A SELECT of the same table raises it too (register #4), and an UPDATE of a
table an earlier statement defined implicitly answers as it always has.
"""

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
error = "The table 'ok' does not exist"

[[test.results]]
value = "[{ id: ex:1 }]"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
value = "[{ id: imp:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: imp3:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"
*/
UPDATE nosuch SET a = 1 WHERE false;
DELETE nosuch WHERE false;
UPDATE type::table('nosuch') SET a = 1 WHERE false;
UPDATE type::table('nosuch') SET a = 1;
DELETE type::table('nosuch');
UPDATE type::table('nosuch') SET a = 1 WHERE a = 1;
UPDATE type::table('nosuch') SET a = 1 WHERE 1 = 2;
BEGIN; UPDATE nosuch SET a = 1 WHERE false; CREATE ok:1; COMMIT; SELECT * FROM ok;
CREATE ex:1; UPDATE ex, nosuch SET a = 1 WHERE false;
UPDATE ONLY nosuch SET a = 1 WHERE false;
UPDATE nosuch SET a = 1 WHERE false RETURN NONE;
LET $f = false; UPDATE nosuch SET a = 1 WHERE $f;
UPDATE nosuch SET a = 1 WHERE 1 = 2;
UPDATE nosuch SET a = 1;
UPDATE nosuch:1 SET a = 1;
UPDATE nosuch:1..5 SET a = 1 WHERE false;
DELETE nosuch:1;
SELECT * FROM nosuch WHERE false;
CREATE imp:1; UPDATE imp SET a = 1 WHERE false;
{ CREATE imp2:1; UPDATE imp2 SET a = 1 WHERE false; };
BEGIN; CREATE imp3:1; UPDATE imp3 SET a = 1 WHERE false; COMMIT;
