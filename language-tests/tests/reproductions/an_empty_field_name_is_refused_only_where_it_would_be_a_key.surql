/**
[test]
reason = "A storage key cannot hold an object with an empty field name, so a record id or an indexed value holding one is refused before anything is written, while an ordinary field value may hold one."

[[test.results]]
error = 'Found { "": 1 } in a record id or an indexed value, but an object there cannot have an empty field name'

[[test.results]]
value = '[{ id: t:1, v: { "": 1 } }]'

[[test.results]]
value = "NONE"

[[test.results]]
error = 'Found { "": 1 } in a record id or an indexed value, but an object there cannot have an empty field name'

[[test.results]]
error = 'Found { "": 1 } in a record id or an indexed value, but an object there cannot have an empty field name'

[[test.results]]
value = '[{ id: t:1, v: { "": 1 } }]'

[env]
namespace = "test"
database = "test"

*/
CREATE t:{ "": 1 };
CREATE t:1 SET v = { "": 1 };
DEFINE INDEX v ON u FIELDS v;
CREATE u:1 SET v = { "": 1 };
DEFINE INDEX v ON t FIELDS v;
SELECT * FROM t, u;
