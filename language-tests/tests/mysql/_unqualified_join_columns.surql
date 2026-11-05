/**
[test]
run = false
reason = "Setup only: three schemaless tables whose columns overlap only in k and dup, with a key column of their own (ak, bk, ck), for the unqualified_join_columns tests of the MySQL dialect."
*/

DEFINE TABLE ujc_a SCHEMALESS;
DEFINE TABLE ujc_b SCHEMALESS;
DEFINE TABLE ujc_c SCHEMALESS;

CREATE ujc_a:1 SET k = 1, ak = 1, v = 'a1', dup = 'da1' RETURN NONE;
CREATE ujc_a:2 SET k = 2, ak = 2, v = 'a2', dup = 'da2' RETURN NONE;
CREATE ujc_a:3 SET k = 3, ak = 3, v = 'a3', dup = 'da3' RETURN NONE;

CREATE ujc_b:1 SET k = 1, bk = 1, w = 'b1', dup = 'db1' RETURN NONE;
CREATE ujc_b:2 SET k = 2, bk = 2, w = 'b2', dup = 'db2' RETURN NONE;
CREATE ujc_b:3 SET k = 2, bk = 2, w = NULL, dup = 'db3' RETURN NONE;
CREATE ujc_b:4 SET k = 4, bk = 4, w = 'b4', dup = 'db4' RETURN NONE;

CREATE ujc_c:1 SET k = 1, ck = 1, x = 'c1' RETURN NONE;
CREATE ujc_c:2 SET k = 5, ck = 5, x = 'c5' RETURN NONE;
