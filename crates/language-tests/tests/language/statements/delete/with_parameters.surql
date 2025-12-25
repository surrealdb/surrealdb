/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:micha }]"

*/

LET $micha = CREATE ONLY person:micha;
DELETE $micha RETURN BEFORE;