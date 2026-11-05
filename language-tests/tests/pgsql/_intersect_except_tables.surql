/**
[test]
run = false
reason = "Setup only: two tables whose rows repeat and hold NULL, and two tables with a column some rows do not have, read by the intersect_except tests"
*/

CREATE ie_left:1 SET v = 1, w = 'x' RETURN NONE;
CREATE ie_left:2 SET v = 1, w = 'x' RETURN NONE;
CREATE ie_left:3 SET v = 2, w = 'y' RETURN NONE;
CREATE ie_left:4 SET v = 3, w = 'z' RETURN NONE;
CREATE ie_left:5 SET v = 3, w = 'z' RETURN NONE;
CREATE ie_left:6 SET v = 3, w = 'z' RETURN NONE;
CREATE ie_left:7 SET v = NULL, w = 'n' RETURN NONE;
CREATE ie_left:8 SET v = NULL, w = 'n' RETURN NONE;

CREATE ie_right:1 SET v = 1, w = 'x' RETURN NONE;
CREATE ie_right:2 SET v = 3, w = 'z' RETURN NONE;
CREATE ie_right:3 SET v = 3, w = 'z' RETURN NONE;
CREATE ie_right:4 SET v = 4, w = 'q' RETURN NONE;
CREATE ie_right:5 SET v = NULL, w = 'n' RETURN NONE;

CREATE ie_gap_left:1 SET a = 1 RETURN NONE;
CREATE ie_gap_left:2 SET a = 2 RETURN NONE;
CREATE ie_gap_right:1 SET a = 1, b = NULL RETURN NONE;
CREATE ie_gap_right:2 SET a = 3, b = 'x' RETURN NONE;
