/**
[test]
reason = "A SCHEMAFULL table with rows (compatibility level 4) keeps its rows consistent with its schema: a new field whose TYPE does not accept NONE is refused, DEFAULT included, unless MIGRATE EXISTING ROWS says what the old rows hold, and then every old row reads that value; a removed field's values go from every row."
issue = 2721

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:emanuele, name: 'Emanuele' }]"

[[test.results]]
error = "Cannot change field `awesomeness` of table `person`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
value = "[{ id: person:emanuele, name: 'Emanuele' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ awesomeness: 10, id: person:emanuele, name: 'Emanuele' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ awesomeness: 10, id: person:emanuele }]"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON TABLE person TYPE string;
CREATE person CONTENT { id: 'emanuele', name: 'Emanuele' };
DEFINE FIELD awesomeness ON TABLE person TYPE int DEFAULT 10;
SELECT * FROM person:emanuele;
DEFINE FIELD awesomeness ON TABLE person TYPE int DEFAULT 10 MIGRATE EXISTING ROWS AS add_awesomeness USING 10;
SELECT * FROM person:emanuele;
REMOVE FIELD name ON TABLE person;
SELECT * FROM person:emanuele;
