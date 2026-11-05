/**
[test]
reason = """
	Setup for the key-range tests of tables whose record ids name their
	components (explain_matrix/record_key_range_*.surql and the
	id_components_* planner tests that import it).

	`reading` is keyed by a city and a datetime: sixty rows, one every 400ms
	from 2024-01-01T00:00:00Z, the cities taking turns, so each city has a row
	every 1.2s and several rows share a whole second. `temp` is the row's
	number, 0 to 59: London holds 0, 3, ..., 57, Paris 1, 4, ..., 58 and Rome
	2, 5, ..., 59, the row numbered i at i * 400ms.

	`counter` is keyed by a site and an int: twenty rows, `n` from -5 to 14,
	site `a` for an even `hits` and `b` for an odd one, `hits` being `n + 5`.
"""
run = false
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
FOR $i IN 0..60 {
	CREATE reading SET
		city = ['London', 'Paris', 'Rome'][$i % 3],
		time = time::from_millis(1704067200000 + $i * 400),
		temp = $i
		RETURN NONE;
};

DEFINE TABLE counter SCHEMALESS;
DEFINE FIELD id ON counter TYPE [site: string, n: int];
FOR $i IN 0..20 {
	CREATE counter SET site = ['a', 'b'][$i % 2], n = $i - 5, hits = $i RETURN NONE;
};
