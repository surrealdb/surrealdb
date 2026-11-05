/**
[env]
namespace = true
database = true

[test]
reason = "Defining a field inside a declared element object leaves that object closed: an undeclared key in it is still refused, unless the element is FLEXIBLE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'obj.a.extra', but no such field exists for table 'closed'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'obj.a.extra', but no such field exists for table 'closed'"

[[test.results]]
value = "[{ id: closed:3, obj: { a: { name: 'x' } } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: open:1, obj: { a: { extra: 1, name: 'x' } } }]"
*/

DEFINE TABLE closed SCHEMAFULL;
DEFINE FIELD obj ON closed TYPE object;
DEFINE FIELD obj.* ON closed TYPE object;
CREATE closed:1 SET obj = { a: { extra: 1 } };
DEFINE FIELD obj.*.name ON closed TYPE string;
CREATE closed:2 SET obj = { a: { name: 'x', extra: 1 } };
CREATE closed:3 SET obj = { a: { name: 'x' } };
DEFINE TABLE open SCHEMAFULL;
DEFINE FIELD obj ON open TYPE object;
DEFINE FIELD obj.* ON open TYPE object FLEXIBLE;
DEFINE FIELD obj.*.name ON open TYPE string;
CREATE open:1 SET obj = { a: { name: 'x', extra: 1 } };
