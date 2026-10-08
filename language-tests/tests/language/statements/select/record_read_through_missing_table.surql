/**
[test]
reason = "Reading a record through a table that does not exist raises, wherever the read happens: a FROM point read or range, a literal array of ids, a mock record set, a parameter holding ids, and a record link followed in value position by a path, `.*` or FETCH. A table that exists but holds no such record reads as no row in FROM and as NONE in value position. `record::exists` answers false rather than raising, since existence is its question."

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
value = "[{ id: present:1, link: missing:1, links: [missing:1, present:1] }]"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
value = "false"

[[test.results]]
value = "[{ id: present:1, link: missing:1, links: [missing:1, present:1] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"
*/
SELECT * FROM missing:1;
SELECT * FROM missing:1..3;
SELECT * FROM [missing:1, missing:2];
SELECT * FROM |missing:3|;
SELECT * FROM |missing:1..3|;
LET $ids = [missing:1];
SELECT * FROM $ids;
RETURN missing:1.name;
RETURN missing:1.*;
CREATE present:1 SET link = missing:1, links = [missing:1, present:1];
SELECT link.name FROM present:1;
SELECT links.*.id FROM present:1;
SELECT * FROM present:1 FETCH link;
RETURN record::exists(missing:1);
SELECT * FROM [present:1, present:9];
SELECT * FROM |present:3|;
RETURN present:9.name;
