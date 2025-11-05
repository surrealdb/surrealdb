/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ brother: true, id: knows:test, in: person:tobie, out: person:jaime }]"

*/

LET $tobie = person:tobie;
LET $jaime = person:jaime;
RELATE $tobie->knows->$jaime SET id = knows:test, brother = true;
