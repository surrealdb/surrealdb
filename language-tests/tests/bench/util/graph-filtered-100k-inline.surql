/**
# Filtered-traversal dataset at 100000 edges: one hub `person` vertex whose
# outgoing `knows` edges carry an INLINE `score`, ~1% of them matching
# `score > 9899`. The 20000-edge `graph-filtered-inline` measures the same
# shape at moderate degree; this dataset is the headline scale for the
# inline-property claim (filtered traversal at 1% selectivity), and doubles
# as the everything-on dataset for `graph/combined_filtered_100k`, which
# folds it into numeric blocks before measuring. The plain twin
# (`graph-filtered-100k-plain`) holds the same graph without the INLINE
# clause. Deterministic scores so every variant selects identically.
# `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows TYPE RELATION;
	DEFINE FIELD score ON knows TYPE number INLINE;

	CREATE |person:1..=100001| RETURN NONE;

	FOR $n IN 1..=100000 {
		LET $t = type::record('person', $n + 1);
		RELATE person:1->knows->$t SET score = $n % 10000;
	};

	RETURN 'OK';
}
