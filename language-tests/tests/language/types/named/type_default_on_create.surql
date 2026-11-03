/**
[env]
namespace = true
database = true

[test]
reason = "A type's DEFAULT fills a field left NONE only when the record is created, and a field's VALUE reads it as $value"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, x: 7, y: 8 }]"

[[test.results]]
value = "[{ id: t:1, y: 9 }]"

[[test.results]]
value = "[{ id: t:1, x: 3, y: 10 }]"

[[test.results]]
value = "[{ id: t:1, y: 11 }]"

[[test.results]]
value = "[{ id: t:2, x: 2, y: 11 }]"
*/

DEFINE TYPE level AS option<int> DEFAULT 7;
DEFINE FIELD x ON t TYPE level;
DEFINE FIELD y ON t TYPE level VALUE $value + 1;
CREATE t:1;
UPDATE t:1 SET x = NONE;
UPDATE t:1 SET x = 3;
UPDATE t:1 SET x = NONE;
CREATE t:2 SET x = 2, y = 10;
