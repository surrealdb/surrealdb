/**
# 200000 persons with an indexed alternating flag, and one hub document
# written by 500 of them (every 400th person). Gives the semi-join bench a
# selective traversal branch beside a selective index branch — the shape
# where per-row traversal pays 200k cursor opens and a reachability bitmap
# pays one 500-key band scan. `run = false`.

[test]
run = false
*/
DEFINE TABLE person SCHEMALESS;
DEFINE FIELD flag ON person TYPE bool;
DEFINE INDEX idx_flag ON person FIELDS flag;
DEFINE TABLE wrote TYPE RELATION;
CREATE doc:hub RETURN NONE;
FOR $i IN 0..200000 {
	CREATE type::record('person', $i) SET flag = $i % 2 == 0 RETURN NONE;
};
FOR $i IN 0..500 {
	RELATE (type::record('person', $i * 400))->wrote->doc:hub RETURN NONE;
};
