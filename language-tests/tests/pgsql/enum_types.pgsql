/**
[env]
namespace = true
database = true

[test]
reason = "CREATE TYPE AS ENUM, ALTER TYPE, DROP TYPE, enum columns, and the pg_type and pg_enum rows of an enum"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, m: 'happy', name: 'a' }, { id: 2, m: 'sad', name: 'b' }, { id: 3, m: 'ok', name: 'c' }]"

[[test.results]]
error = "Couldn't coerce value for field `m` of `person:4`: Expected `mood ('sad' | 'ok' | 'happy')` but found `'nope'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ enumlabel: 'blue' }, { enumlabel: 'meh' }, { enumlabel: 'ok' }, { enumlabel: 'happy' }, { enumlabel: 'ecstatic' }]"

[[test.results]]
value = "[{ typname: 'mood', typtype: 'e' }]"

[[test.results]]
value = "[{ labels: 5 }]"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the field 'm' on the table 'person' has a type that names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

CREATE TABLE person (id int PRIMARY KEY, name text, m mood);

INSERT INTO person (id, name, m) VALUES (1, 'a', 'happy'), (2, 'b', 'sad'), (3, 'c', 'ok');

INSERT INTO person (id, name, m) VALUES (4, 'd', 'nope');

ALTER TYPE mood ADD VALUE 'ecstatic' AFTER 'happy';

ALTER TYPE mood ADD VALUE IF NOT EXISTS 'ecstatic';

ALTER TYPE mood ADD VALUE 'meh' BEFORE 'ok';

ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';

SELECT enumlabel FROM pg_enum ORDER BY enumsortorder;

SELECT typname, typtype FROM pg_type WHERE typname = 'mood';

SELECT count(*) AS labels FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid WHERE t.typname = 'mood';

DROP TYPE mood;

DROP TABLE person;

DROP TYPE mood;

DROP TYPE IF EXISTS mood;
