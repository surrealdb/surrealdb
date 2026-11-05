/**
[env]
namespace = true
database = true
imports = ["pgsql/_native.surql"]

[test]
reason = "PostgreSQL reads that raise: a missing table on either side of a join, with and without rows on the other"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
value = "[]"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"
*/

SELECT e.name FROM missing e;

SELECT e.name, m.x FROM emp e JOIN missing m ON e.dept = m.did;

SELECT e.name, m.x FROM emp e LEFT JOIN missing m ON e.dept = m.did;

SELECT m.x, e.name FROM missing m JOIN emp e ON e.dept = m.did;

SELECT x.id, m.x FROM empty x JOIN missing m ON x.did = m.did;

SELECT x.id, m.x FROM missing m JOIN empty x ON x.did = m.did;

SELECT count(*) AS n FROM missing m;

SELECT e.name FROM emp e UNION SELECT m.x AS name FROM missing m;
