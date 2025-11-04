/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ brother: true, id: knows:test, in: person:tobie, out: person:jaime }]"

[[test.results]]
value = "[{ id: knows:test, in: person:tobie, out: person:jaime, test: true }]"

[[test.results]]
value = "[{ id: knows:test, in: person:tobie, out: person:jaime, test: true }]"

*/

LET $tobie = person:tobie;
LET $jaime = person:jaime;
RELATE $tobie->knows->$jaime CONTENT { id: knows:test, brother: true };
UPDATE knows:test CONTENT { test: true };
SELECT * FROM knows:test;
