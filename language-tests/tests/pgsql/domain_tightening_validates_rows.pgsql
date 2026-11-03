/**
[env]
namespace = true
database = true

[test]
reason = "ALTER DOMAIN ADD CONSTRAINT and SET NOT NULL are refused while a stored value violates them, leaving the domain as it was; a constraint every stored value satisfies is added and judges later writes"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 500i32, id: 1, m: NULL, t: 5i32 }, { a: 7i32, id: 2, m: 3i32, t: 9i32 }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'amount': the field 'a' on the table 'ledger' holds a value it does not admit: Expected `amount (constraint small failed)` but found `500i32`"

[[test.results]]
value = "[{ a: 900i32, id: 3, m: NULL, t: NULL }]"

[[test.results]]
error = "Invalid statement: Cannot change the type 'maybe': the field 'm' on the table 'ledger' holds a value it does not admit: Expected `maybe (i32)` but found `NULL`"

[[test.results]]
value = "[{ a: 1i32, id: 4, m: NULL, t: NULL }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `t` of `ledger:5`: Expected `tidy (constraint positive failed)` but found `-1i32`"

[[test.results]]
value = "[{ a: 500i32, id: 1, m: NULL, t: 5i32 }, { a: 7i32, id: 2, m: 3i32, t: 9i32 }, { a: 900i32, id: 3, m: NULL, t: NULL }, { a: 1i32, id: 4, m: NULL, t: NULL }]"

*/

CREATE DOMAIN amount AS integer;

CREATE DOMAIN maybe AS integer;

CREATE DOMAIN tidy AS integer;

CREATE TABLE ledger (id int PRIMARY KEY, a amount, m maybe, t tidy);

INSERT INTO ledger (id, a, m, t) VALUES (1, 500, NULL, 5), (2, 7, 3, 9);

ALTER DOMAIN amount ADD CONSTRAINT small CHECK (VALUE < 100);

INSERT INTO ledger (id, a) VALUES (3, 900);

ALTER DOMAIN maybe SET NOT NULL;

INSERT INTO ledger (id, a) VALUES (4, 1);

ALTER DOMAIN tidy ADD CONSTRAINT positive CHECK (VALUE > 0);

INSERT INTO ledger (id, t) VALUES (5, -1);

SELECT id, a, m, t FROM ledger ORDER BY id;
