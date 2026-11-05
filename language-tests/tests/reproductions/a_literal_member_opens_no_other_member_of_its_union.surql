/**
[env]
namespace = true
database = true

[test]
reason = """
In a SCHEMAFULL table, each place beneath a field's value is judged by the kind that describes it,
as that kind would be judged alone. A literal object admits the keys it names, a literal array the
elements it describes, and an object admits a key no definition names only when the field is
FLEXIBLE. A union judges a value by each member it is an instance of, so a literal member opens
nothing beneath a value of another member, and an object a literal holds is closed as an object the
field holds directly. A definition of a place a literal names overrides what the literal says of it.
An import does not coerce what it replays, and a value of a union kind that is an instance of no
member is judged by every member.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'v.x', but no such field exists for table 'closed_object'"

[[test.results]]
value = "[{ id: closed_object:literal, v: [1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: named_keys:named, v: { a: 1 } }]"

[[test.results]]
error = "Found field 'v.b', but no such field exists for table 'named_keys'"

[[test.results]]
error = "Found field 'v.a', but no such field exists for table 'named_keys'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'v[0].x', but no such field exists for table 'array_member'"

[[test.results]]
value = "[{ id: array_member:empty, v: [{  }] }]"

[[test.results]]
value = "[{ id: array_member:string, v: 'text' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'v.x', but no such field exists for table 'scalar_literal'"

[[test.results]]
value = "[{ id: scalar_literal:literal, v: 'unset' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: flexible_member:object, v: { x: 1 } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: literal_slots:empty, v: { l: [{  }], m: {  }, o: {  } } }]"

[[test.results]]
error = "Found field 'v.o.x', but no such field exists for table 'literal_slots'"

[[test.results]]
error = "Found field 'v.l[0].x', but no such field exists for table 'literal_slots'"

[[test.results]]
error = "Found field 'v.m.x', but no such field exists for table 'literal_slots'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: flexible_slots:1, v: { l: [{ y: 2 }], o: { x: 1 } } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: defined_slot:open, v: { o: { x: 1 }, p: {  } } }]"

[[test.results]]
error = "Found field 'v.p.x', but no such field exists for table 'defined_slot'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: literal_elements:empty, v: [{ a: {  } }] }]"

[[test.results]]
error = "Found field 'v[0].a.x', but no such field exists for table 'literal_elements'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Found field 'v.c', but no such field exists for table 'imported'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: imported:replayed, v: { a: { x: 1 }, b: 'text' } }]"

*/

-- An object member is closed beside a literal member.
DEFINE TABLE closed_object SCHEMAFULL;
DEFINE FIELD v ON closed_object TYPE object | [int];
CREATE closed_object:object SET v = { x: 1 };
CREATE closed_object:literal SET v = [1];

-- The literal member admits the keys it names, for a value that is an instance of it.
DEFINE TABLE named_keys SCHEMAFULL;
DEFINE FIELD v ON named_keys TYPE object | { a: int };
CREATE named_keys:named SET v = { a: 1 };
CREATE named_keys:other SET v = { b: 1 };
-- An extra key makes the value an instance of `object` alone, which names no key.
CREATE named_keys:both SET v = { a: 1, b: 2 };

-- The objects of an array member are closed beside a literal member.
DEFINE TABLE array_member SCHEMAFULL;
DEFINE FIELD v ON array_member TYPE string | array<object> | { a: int };
CREATE array_member:array SET v = [{ x: 1 }];
CREATE array_member:empty SET v = [{}];
CREATE array_member:string SET v = 'text';

-- A literal scalar member opens nothing either.
DEFINE TABLE scalar_literal SCHEMAFULL;
DEFINE FIELD v ON scalar_literal TYPE 'unset' | object;
CREATE scalar_literal:object SET v = { x: 1 };
CREATE scalar_literal:literal SET v = 'unset';

-- FLEXIBLE opens the object member.
DEFINE TABLE flexible_member SCHEMAFULL;
DEFINE FIELD v ON flexible_member TYPE object | [int] FLEXIBLE;
CREATE flexible_member:object SET v = { x: 1 };

-- An object a literal holds is closed as one the field holds directly.
DEFINE TABLE literal_slots SCHEMAFULL;
DEFINE FIELD v ON literal_slots TYPE { o: object, l: array<object>, m: option<object> };
CREATE literal_slots:empty SET v = { o: {}, l: [{}], m: {} };
CREATE literal_slots:key SET v = { o: { x: 1 }, l: [] };
CREATE literal_slots:element SET v = { o: {}, l: [{ x: 1 }] };
CREATE literal_slots:optional SET v = { o: {}, l: [], m: { x: 1 } };

-- FLEXIBLE opens the objects a literal holds.
DEFINE TABLE flexible_slots SCHEMAFULL;
DEFINE FIELD v ON flexible_slots TYPE { o: object, l: array<object> } FLEXIBLE;
CREATE flexible_slots:1 SET v = { o: { x: 1 }, l: [{ y: 2 }] };

-- A definition of a key the literal names overrides what the literal says of it.
DEFINE TABLE defined_slot SCHEMAFULL;
DEFINE FIELD v ON defined_slot TYPE { o: object, p: object };
DEFINE FIELD v.o ON defined_slot TYPE object FLEXIBLE;
CREATE defined_slot:open SET v = { o: { x: 1 }, p: {} };
CREATE defined_slot:closed SET v = { o: {}, p: { x: 1 } };

-- The elements of an array are judged by the literal of its element kind.
DEFINE TABLE literal_elements SCHEMAFULL;
DEFINE FIELD v ON literal_elements TYPE string | array<{ a: object }>;
CREATE literal_elements:empty SET v = [{ a: {} }];
CREATE literal_elements:key SET v = [{ a: { x: 1 } }];

-- An import replays a value that is an instance of no member, judged by every member.
DEFINE TABLE imported SCHEMAFULL;
DEFINE FIELD v ON imported TYPE { a: any, b: int } | string;
OPTION IMPORT;
INSERT [{ id: imported:replayed, v: { a: { x: 1 }, b: 'text' } }];
INSERT [{ id: imported:undescribed, v: { a: {}, b: 'text', c: 1 } }];
OPTION IMPORT = false;
SELECT * FROM imported ORDER BY id;
