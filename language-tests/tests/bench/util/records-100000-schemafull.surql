/**
# 100k records with sequential ids and deterministic values, in a SCHEMAFULL table, for the benches
# whose subject is the compute half of a write: no field is drawn at random, so a write over it
# computes what it computes for every record whatever the width it runs at. `run = false` keeps it
# out of the regular suite.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE record SCHEMAFULL;
	DEFINE FIELD n ON record TYPE int;
	DEFINE FIELD name ON record TYPE string;
	DEFINE FIELD score ON record TYPE float;
	DEFINE FIELD city ON record TYPE string;
	DEFINE FIELD h ON record TYPE option<string>;
	DEFINE FIELD slug ON record TYPE option<string>;
	DEFINE FIELD w ON record TYPE option<array<string>>;

	FOR $i IN 1..100001 {
		CREATE type::record('record', $i)
			SET n = $i,
				name = 'n' + <string> $i,
				score = $i * 0.5,
				city = ['London', 'Paris', 'Berlin'][$i % 3]
			RETURN NONE;
	};

	RETURN 'OK';
}
