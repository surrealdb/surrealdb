/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "the statement of the bench update_compute_100k over a SCHEMAFULL table of 2000 records answers as the serial run does, whatever width its compute half runs at"

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
value = "[{ city: 'Paris', h: 'cf795a2cfe8d71265d2422678f4aa3f61d8cd2d021a7d35077153ded1829ac93', id: record:1999, n: 1999, name: 'n1999', score: 999.5f, slug: 'n1999', w: ['n1999'] }]"

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
UPDATE record SET h = crypto::sha256(name + <string> n), slug = string::slug(name), w = string::words(name) RETURN NONE;
SELECT count() FROM record WHERE h != NONE AND slug != NONE AND w != NONE GROUP ALL;
SELECT * FROM record:1999;
