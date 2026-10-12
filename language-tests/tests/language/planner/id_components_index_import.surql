/**
[test]
reason = """
	Setup for the tests of indexes over the named components of a record id
	(explain_matrix/index_on_id_component.surql and the
	statements/define/index/id_components_*.surql tests that import it).

	`tick` is keyed by a feed and a datetime. Eleven rows, `n` being the row's
	number, all on 2024-01-01 and listed here by the time of day:

	  feed f: n0 00:00:00, n1 00:00:00.900, n2 00:00:00.500, n3 00:00:00.050,
	          n4 00:00:01.250, n5 00:00:00.999999999, n6 00:00:01,
	          n7 00:00:02.100
	  feed g: n8 00:00:00.500, n9 00:00:01, n10 00:00:03

	The times cluster around the whole seconds 00:00:00, 00:00:01 and
	00:00:02 on purpose: an index entry for a datetime orders by its whole
	second only, so a bound that is itself a whole second, a bound a
	nanosecond from one and a bound inside one are each a case a range can
	get wrong. `evt` is keyed by a source and an int: six rows, `n` 0 to 5,
	source `a` for an even `n` and `b` for an odd one, `v` being `n * 10`.
"""
run = false
*/

DEFINE TABLE tick SCHEMALESS;
DEFINE FIELD id ON tick TYPE [feed: string, at: datetime];
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00Z', n = 0 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.900Z', n = 1 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.500Z', n = 2 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.050Z', n = 3 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:01.250Z', n = 4 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.999999999Z', n = 5 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:01Z', n = 6 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:02.100Z', n = 7 RETURN NONE;
CREATE tick SET feed = 'g', at = d'2024-01-01T00:00:00.500Z', n = 8 RETURN NONE;
CREATE tick SET feed = 'g', at = d'2024-01-01T00:00:01Z', n = 9 RETURN NONE;
CREATE tick SET feed = 'g', at = d'2024-01-01T00:00:03Z', n = 10 RETURN NONE;

DEFINE TABLE evt SCHEMALESS;
DEFINE FIELD id ON evt TYPE [src: string, n: int];
FOR $i IN 0..6 {
	CREATE evt SET src = ['a', 'b'][$i % 2], n = $i, v = $i * 10 RETURN NONE;
};
