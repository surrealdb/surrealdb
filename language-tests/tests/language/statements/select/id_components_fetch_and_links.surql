/**
[env]
imports = ["./id_components_import.surql"]

[test]
reason = """
	A record link to a table whose id names its components reads them off the
	linked record, in a projection, in a predicate, and through FETCH: a
	fetched record carries them as it carries its computed fields. A link to a
	record that does not exist reads NONE.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ city: 'Paris', name: 'a', temp: 7 }, { city: NONE, name: 'b', temp: NONE }]"

[[test.results]]
value = "[{ name: 'a' }]"

[[test.results]]
value = "[{ id: trip:a, last: { city: 'Paris', id: reading:['Paris', d'2024-01-03T00:00:00Z'], tag: 'Paris-7', temp: 7, time: d'2024-01-03T00:00:00Z' }, name: 'a' }]"
*/

DEFINE TABLE trip SCHEMAFULL;
DEFINE FIELD name ON trip TYPE string;
DEFINE FIELD last ON trip TYPE option<record<reading>>;
CREATE trip:a SET name = 'a', last = reading:['Paris', d'2024-01-03T00:00:00Z'] RETURN NONE;
-- A link to a record that was never written.
CREATE trip:b SET name = 'b', last = reading:['Rome', d'2024-01-01T00:00:00Z'] RETURN NONE;

-- A component of the linked record in a projection; the dangling link has none.
SELECT name, last.city AS city, last.temp AS temp FROM trip ORDER BY name;

-- A component of the linked record in a predicate.
SELECT name FROM trip WHERE last.city = 'Paris';

-- FETCH replaces the link with the record, components included.
SELECT * FROM trip:a FETCH last;
