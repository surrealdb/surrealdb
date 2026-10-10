/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
Over a set of objects or of record links, a field read after `.*` (`s.*.x`,
`tags.*.name`), or directly (`s.x`), is read on each element and the values are
collected into a set, as over an array; so is a destructure (`s.{x}`,
`s.*.{x}`). Per element, a missing field reads as NONE, an element that is
not an object destructures to NONE, and a record link is fetched; an empty set
maps to an empty set. A containment over such a path, with
the path indexed by `FIELDS s.*.x` or `FIELDS tags.*.name`, returns every
matching record on every engine, and SELECT, count() and an UPDATE's condition
agree.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, s: {{ x: 1 }, { x: 2 }} }]"

[[test.results]]
value = "[{ id: t:2, s: {{ x: 2 }, { x: 3 }} }]"

[[test.results]]
value = "[{1, 2}, {2, 3}]"

[[test.results]]
value = "[{1, 2}, {2, 3}]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tag:go, name: 'go' }]"

[[test.results]]
value = "[{ id: tag:rust, name: 'rust' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{'go', 'rust'}, {'go',}]"

[[test.results]]
value = "[post:1]"

[[test.results]]
value = "[post:1, post:2]"

[[test.results]]
value = "[{{ x: 1 }, { x: 2 }}]"

[[test.results]]
value = "[{{ x: 1 }, { x: 2 }}]"

[[test.results]]
value = "{NONE, 1, 2}"

[[test.results]]
value = "{,}"

[[test.results]]
value = "{{ a: NONE }, { a: 1 }}"

[[test.results]]
value = "{NONE,}"

[[test.results]]
value = "{{ name: 'go' }, { name: 'rust' }}"

[[test.results]]
value = "{NONE, 42, { id: tag:go, name: 'go' }}"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD s ON t TYPE set<object>;
DEFINE INDEX ix ON t FIELDS s.*.x;
CREATE t:1 SET s = <set>[{ x: 1 }, { x: 2 }];
CREATE t:2 SET s = <set>[{ x: 2 }, { x: 3 }];
SELECT VALUE s.*.x FROM t ORDER BY id;
SELECT VALUE s.x FROM t ORDER BY id;
SELECT VALUE id FROM t WHERE s.*.x CONTAINS 1 ORDER BY id;
SELECT VALUE id FROM t WHERE s.*.x CONTAINS 2 ORDER BY id;
SELECT VALUE id FROM t WHERE 3 INSIDE s.*.x ORDER BY id;
SELECT VALUE id FROM t WHERE s.x CONTAINS 3 ORDER BY id;
SELECT count() FROM t WHERE s.*.x CONTAINS 2 GROUP ALL;
UPDATE t SET hit = true WHERE s.*.x CONTAINS 1 RETURN VALUE id;

-- A set of record links, read through a field of each linked record.
DEFINE TABLE tag SCHEMALESS;
CREATE tag:go SET name = 'go';
CREATE tag:rust SET name = 'rust';
DEFINE TABLE post SCHEMALESS;
DEFINE FIELD tags ON post TYPE set<record<tag>>;
DEFINE INDEX ixt ON post FIELDS tags.*.name;
CREATE post:1 SET tags = <set>[tag:rust, tag:go] RETURN NONE;
CREATE post:2 SET tags = <set>[tag:go] RETURN NONE;
SELECT VALUE tags.*.name FROM post ORDER BY id;
SELECT VALUE id FROM post WHERE tags.*.name CONTAINS 'rust' ORDER BY id;
SELECT VALUE id FROM post WHERE tags.*.name CONTAINS 'go' ORDER BY id;

-- A destructure over a set, directly or after `.*`, is taken per element.
SELECT VALUE s.{x} FROM t:1;
SELECT VALUE s.*.{x} FROM t:1;

-- Per element: a missing field, a non-object, an empty set, a link and a dangling link.
RETURN (<set>[{ a: 1 }, { a: 2 }, { b: 3 }]).a;
RETURN (<set>[]).{ a, b };
RETURN (<set>[{ a: 1 }, {}]).{ a };
RETURN (<set>[1, 2]).{ a };
RETURN (<set>[tag:rust, tag:go]).{ name };
RETURN (<set>[tag:go, 42, tag:nope]).*;
