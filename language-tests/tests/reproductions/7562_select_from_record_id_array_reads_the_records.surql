/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A record id in `FROM` is a source to read, whether it is written inline, held in
an array, or held in a parameter: each id is fetched, a row is the stored
record, and an id with no stored record yields no row, as `FROM item:2` does.
`$this` in the projection is that record, so a function typed `record<item>`
called with `$this` is handed an object and refuses it; `id` is the record id.

Iterating the ids themselves, keeping the missing ones, is a value operation on
the array: `.map(...)` keeps every element and never reads a record.
"""
issue = 7562

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ mapped: [item:1, item:2], selected: [{ id: item:1, value: 1 }] }"

[[test.results]]
value = "{ mapped: [{ ref: item:1 }, { ref: item:2 }], selected: [{ ref: { id: item:1, value: 1 } }] }"

[[test.results]]
value = "[{ id: item:1, value: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "'item:1'"

[[test.results]]
error = "Incorrect arguments for function fn::repro(). Failed to coerce argument `$id`: Expected `record<item>` but found `{ id: item:1, value: 1 }`"

[[test.results]]
value = "['item:1']"

[[test.results]]
value = "['item:1', 'item:2']"

*/
DEFINE TABLE item SCHEMALESS;
CREATE item:1 SET value = 1 RETURN NONE;
DEFINE FUNCTION fn::repro($id: record<item>) { RETURN type::string($id); };
LET $ids = [item:1, item:2];

-- A parameter holding ids reads the records; the missing id yields no row.
RETURN { selected: (SELECT VALUE $this FROM $ids), mapped: $ids.map(|$id| $id) };
RETURN { selected: (SELECT $this AS ref FROM $ids), mapped: $ids.map(|$id| { ref: $id }) };

-- The same ids written inline read the same records.
SELECT VALUE $this FROM [item:1, item:2];
SELECT * FROM item:2;

-- `$this` is the record, so a `record<item>` argument refuses it.
RETURN fn::repro($ids[0]);
RETURN SELECT VALUE fn::repro($this) FROM $ids;

-- `id` is the record id; `.map` keeps every id, read or not.
SELECT VALUE fn::repro(id) FROM $ids;
RETURN $ids.map(|$id| fn::repro($id));
