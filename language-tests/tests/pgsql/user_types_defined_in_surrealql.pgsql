/**
[env]
namespace = true
database = true
imports = ["pgsql/_user_types_surrealql_import.surql"]

[test]
reason = "A domain and a composite type that SurrealQL's DEFINE TYPE defines, as an export replays them, are the types the PostgreSQL front end declares columns with and drops"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 30i32, home: { street: 'Main St', zip: 12345i32 }, id: 1 }]"

[[test.results]]
error = "Couldn't coerce value for field `age` of `person:2`: Expected `posint (constraint positive failed)` but found `-1i32`"

[[test.results]]
error = "Couldn't coerce value for field `age` of `person:3`: Expected `posint (i32)` but found `NULL`"

[[test.results]]
value = "[{ age: 30i32, id: 1, zip: 12345i32 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

CREATE TABLE person (id int PRIMARY KEY, age posint, home address);

INSERT INTO person (id, age, home) VALUES (1, 30, ROW('Main St', 12345)::address);

INSERT INTO person (id, age) VALUES (2, -1);

INSERT INTO person (id, age) VALUES (3, NULL);

SELECT id, age, (home).zip AS zip FROM person ORDER BY id;

DROP TABLE person;

DROP DOMAIN posint;

DROP TYPE address;
