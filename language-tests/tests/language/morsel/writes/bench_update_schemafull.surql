/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "the statement of the bench update_schemafull_100k over a SCHEMAFULL table of 2000 records answers as the serial run does, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2000 }]"

[[test.results]]
value = "[{ total: 2001000f }]"

[[test.results]]
value = "[{ city: 'Berlin', id: record:2000, n: 2000, name: 'n2000 hi!', score: 2000f }]"

*/

DEFINE TABLE record SCHEMAFULL;
DEFINE FIELD n ON record TYPE int;
DEFINE FIELD name ON record TYPE string;
DEFINE FIELD score ON record TYPE float;
DEFINE FIELD city ON record TYPE string;
DEFINE FIELD h ON record TYPE option<string>;
DEFINE FIELD slug ON record TYPE option<string>;
DEFINE FIELD w ON record TYPE option<array<string>>;
FOR $i IN 1..2001 {
	CREATE type::record('record', $i)
		SET n = $i,
			name = 'n' + <string> $i,
			score = $i * 0.5,
			city = ['London', 'Paris', 'Berlin'][$i % 3]
		RETURN NONE;
};
UPDATE record SET name = name + ' hi!', score = score * 2 RETURN NONE;
SELECT count() FROM record WHERE string::ends_with(name, ' hi!') GROUP ALL;
SELECT math::sum(score) AS total FROM record GROUP ALL;
SELECT * FROM record:2000;
