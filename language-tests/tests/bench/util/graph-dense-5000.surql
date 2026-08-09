/**
# Dense graph dataset: 5000 `person` nodes with ~10 `knows` edges each, for
# stressing multi-hop and recursive traversals (per-hop work grows with the
# node degree). Built with RELATE so the adjacency keys exist. `run = false`.

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
		FOR $_ IN array::range(1, 10) {
			LET $t = type::record('person', rand::int(1, 5000));
			RELATE $p->knows->$t;
		};
	};

	RETURN 'OK';
}
