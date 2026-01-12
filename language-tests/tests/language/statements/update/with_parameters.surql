/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

*/

LET $tobie = CREATE ONLY person:tobie;
UPDATE $tobie SET name = "Tobie";