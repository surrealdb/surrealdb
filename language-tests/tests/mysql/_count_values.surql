/**
[test]
run = false
reason = "Setup only: values that are falsy but present (0, false, '' and []), a NULL and a missing value across three groups, read by count_column_values"
*/

CREATE cv_t:1 SET grp = 'x', v = 0 RETURN NONE;
CREATE cv_t:2 SET grp = 'x', v = false RETURN NONE;
CREATE cv_t:3 SET grp = 'x', v = '' RETURN NONE;
CREATE cv_t:4 SET grp = 'x', v = NULL RETURN NONE;
CREATE cv_t:5 SET grp = 'y' RETURN NONE;
CREATE cv_t:6 SET grp = 'y', v = [] RETURN NONE;
CREATE cv_t:7 SET grp = 'z', v = NULL RETURN NONE;
