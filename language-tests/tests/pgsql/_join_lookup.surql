/**
[test]
run = false
reason = "Setup only: a left table of 120 rows beside a few rows whose key is a numeric of another type, a string, NONE, NULL or an array, and a right table indexed on its key with a row of each of those kinds, for join_indexed_lookup.pgsql."
*/

DEFINE TABLE a SCHEMALESS;
DEFINE TABLE b SCHEMALESS;
DEFINE INDEX b_k ON b FIELDS k;

FOR $i IN 0..120 { CREATE type::record('a', $i) SET n = $i, k = $i * 2 RETURN NONE; };
CREATE a:x1 SET n = -1, k = 1 RETURN NONE;
CREATE a:x2 SET n = -2, k = 1.0f RETURN NONE;
CREATE a:x3 SET n = -3 RETURN NONE;
CREATE a:x4 SET n = -4, k = NULL RETURN NONE;
CREATE a:x5 SET n = -5, k = [1] RETURN NONE;

FOR $i IN 0..150 { CREATE type::record('b', $i) SET k = $i, s = $i RETURN NONE; };
CREATE b:f1 SET k = 1.0f, s = 1001 RETURN NONE;
CREATE b:d1 SET k = 1dec, s = 1002 RETURN NONE;
CREATE b:str SET k = '1', s = 1003 RETURN NONE;
CREATE b:nul SET k = NULL, s = 1004 RETURN NONE;
CREATE b:arr SET k = [1], s = 1005 RETURN NONE;
CREATE b:none SET s = 1006 RETURN NONE;
