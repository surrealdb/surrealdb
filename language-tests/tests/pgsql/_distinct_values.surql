/**
[test]
run = false
reason = "Setup only: rows with a NULL, a missing and a repeated value, and one table of the same number written as an int, a float and a decimal, read by count_distinct_skips_null"
*/

CREATE dv_t:1 SET k = 1, v = 1, grp = 'x' RETURN NONE;
CREATE dv_t:2 SET k = 2, v = 1, grp = 'x' RETURN NONE;
CREATE dv_t:3 SET k = 3, v = NULL, grp = 'x' RETURN NONE;
CREATE dv_t:4 SET k = 4, grp = 'y' RETURN NONE;
CREATE dv_t:5 SET k = 5, v = 3, grp = 'y' RETURN NONE;
CREATE dv_t:6 SET k = 6, v = NULL, grp = 'z' RETURN NONE;
CREATE dv_n:1 SET k = 1 RETURN NONE;
CREATE dv_n:2 SET k = 1.0 RETURN NONE;
CREATE dv_n:3 SET k = 1dec RETURN NONE;
CREATE dv_n:4 SET k = 2 RETURN NONE;
