/**

[env]
imports = ["./absent_table_permissions_import.surql"]

auth = { namespace = "test", database = "test", access = "user", rid = "user:1" }

[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

-- A table the catalog does not hold is defined by the write with every
-- permission NONE, so a record user creates nothing in it; dynamic targets
-- and INSERT are judged the same way.
CREATE nowhere:1 SET n = 1;
CREATE type::record('nowhere', 2) SET n = 2;
INSERT INTO nowhere { id: nowhere:3, n: 3 };
RELATE user:1->nowhere:4->user:1 SET n = 4;
