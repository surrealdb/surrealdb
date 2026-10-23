/**
[test]
run = false
reason = "Setup only: rows with a NULL and a missing column, read by cast_of_null"
*/

CREATE agg_t:1 SET k = 1, name = 'ada', age = 36, grp = 'x' RETURN NONE;
CREATE agg_t:2 SET k = 2, name = 'bob', age = NULL, grp = 'x' RETURN NONE;
CREATE agg_t:3 SET k = 3, name = 'cy', age = 29, grp = 'y' RETURN NONE;
CREATE agg_t:4 SET k = 4, name = 'dan', age = NULL, grp = 'z' RETURN NONE;
CREATE agg_t:5 SET k = 5, name = 'eve', grp = 'z' RETURN NONE;
