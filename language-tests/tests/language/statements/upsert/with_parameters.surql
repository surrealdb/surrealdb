/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime' }]"

*/

LET $jaime = UPSERT ONLY person:jaime;
UPSERT $jaime SET name = "Jaime";