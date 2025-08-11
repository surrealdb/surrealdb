/**
[test]
wip = true

[[test.results]]
value = "[{ id: person:test, can_drive: NONE }]"

[[test.results]]
value = "[{ id: person:test, birthday: d'2024-06-22T00:00:00Z', can_drive: false }]"

[[test.results]]
value = "[{ id: person:test, birthday: d'2001-06-22T00:00:00Z', can_drive: true }]"

*/

DEFINE FIELD can_drive ON person VALUE COMPUTED { birthday && d'2025-06-22T00:00:00Z' > birthday + 18y };
UPSERT person:test SET birthday = <datetime> '2024-06-22';
UPSERT person:test SET birthday = <datetime> '2001-06-22';
