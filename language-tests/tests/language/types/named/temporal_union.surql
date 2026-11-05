/**
[env]
namespace = true
database = true

[test]
reason = "A date written to a field of a union that holds both datetime and date stays a date: the member a value already is wins over one that converts it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: pd'2024-01-01', g: d'2024-01-01T10:00:00Z', id: t:1 }]"

[[test.results]]
value = "[{ f: d'2024-01-01T10:00:00Z', g: ts'2024-01-01T00:00:00', id: t:2 }]"
*/

DEFINE FIELD f ON t TYPE datetime | date;
DEFINE FIELD g ON t TYPE timestamp | datetime;
CREATE t:1 SET f = <date>'2024-01-01', g = d'2024-01-01T10:00:00Z';
CREATE t:2 SET f = d'2024-01-01T10:00:00Z', g = <date>'2024-01-01';
