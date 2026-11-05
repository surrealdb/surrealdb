/**
[env]
namespace = true
database = true

[test]
reason = "The declared type of the key column is enforced on the record id: an over-long varchar key is refused, and excess trailing spaces are dropped from the key"

[[test.results]]
value = "NONE"

[[test.results]]
error = "value too long for type character varying(3) (field `id` of `kt:abcd`, SQL TYPE varchar(3))"

[[test.results]]
value = "[{ id: 'ab ', x: 2i32 }]"

[[test.results]]
value = "[{ id: 'ab ', x: 2i32 }]"
*/

CREATE TABLE kt (id VARCHAR(3) PRIMARY KEY, x INT);
INSERT INTO kt (id, x) VALUES ('abcd', 1);
INSERT INTO kt (id, x) VALUES ('ab   ', 2);
SELECT id, x FROM kt;
