/**
# 200000 records over three categories with an alternating flag, both
# indexed at the doc-ID entry format. Gives the planner OR benches two
# large overlapping branches: `category = 'red'` selects ~66k rows,
# `flag = true` selects 100k, and every even red row satisfies both — the
# dedupe-heavy shape where merge-based OR plans pay per duplicate.
# `run = false`.

[test]
run = false
*/
DEFINE TABLE items SCHEMALESS;
DEFINE FIELD category ON items TYPE string;
DEFINE FIELD flag ON items TYPE bool;
DEFINE INDEX idx_category ON items FIELDS category;
DEFINE INDEX idx_flag ON items FIELDS flag;
FOR $i IN 0..200000 {
	CREATE type::record('items', $i) SET
		category = ['red', 'green', 'blue'][$i % 3],
		flag = $i % 2 == 0
	RETURN NONE;
};
