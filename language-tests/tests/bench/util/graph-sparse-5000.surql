/**
# Graph dataset for the traversal benches: 5000 `person` nodes, each with ~2
# random `knows` edges (created via RELATE so the graph adjacency keys exist;
# raw INSERT into the edge table would skip them). `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;

	CREATE |person:1..=5000| SET
		name = rand::string(12),
		age = rand::int(18, 80)
	RETURN NONE;

	FOR $p IN (SELECT VALUE id FROM person) {
		LET $a = type::record('person', rand::int(1, 5000));
		LET $b = type::record('person', rand::int(1, 5000));
		RELATE $p->knows->$a;
		RELATE $p->knows->$b;
	};

	RETURN 'OK';
}
