/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ birthday: d'2007-06-22T00:00:00Z', can_drive: true, id: person:1 }]"

[[test.results]]
value = "[{ birthday: d'2008-06-22T00:00:00Z', can_drive: false, id: person:2 }]"

*/
DEFINE FIELD birthday ON TABLE person TYPE datetime;
DEFINE FIELD can_drive ON TABLE person COMPUTED d"2025-06-22" > birthday + 18y;

CREATE person:1 SET birthday = d"2007-06-22";
CREATE person:2 SET birthday = d"2008-06-22";