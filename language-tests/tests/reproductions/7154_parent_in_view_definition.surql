/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "$parent in nested SELECT inside DEFINE TABLE AS view should resolve like a direct SELECT (#7154)"
issue = 7154

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u7154:s1, name: 'Alice' }]"

[[test.results]]
value = "[{ id: u7154:s2, name: 'Bob' }]"

[[test.results]]
value = "[{ active: true, id: i7154:i1, refs: [u7154:s1, u7154:s2] }]"

[[test.results]]
value = "[{ active: true, id: i7154:i2 }]"

[[test.results]]
value = "[{ id: v7154_a:i1, raw_refs: [u7154:s1, u7154:s2], resolved_refs: [{ id: u7154:s1 }, { id: u7154:s2 }] }, { id: v7154_a:i2, raw_refs: NONE, resolved_refs: NONE }]"

[[test.results]]
value = "[{ id: i7154:i1, resolved_refs: [{ id: u7154:s1 }, { id: u7154:s2 }] }, { id: i7154:i2, resolved_refs: NONE }]"

*/


DEFINE TABLE u7154 SCHEMAFULL TYPE NORMAL;
DEFINE FIELD name ON u7154 TYPE string;

DEFINE TABLE i7154 SCHEMAFULL TYPE NORMAL;
DEFINE FIELD refs ON i7154 TYPE option<array<record<u7154>>>;
DEFINE FIELD active ON i7154 TYPE bool DEFAULT true;

DEFINE TABLE v7154_a TYPE NORMAL AS
	SELECT
		id,
		IF type::is_none(refs) THEN NONE
		ELSE (SELECT VALUE { id: $this.id } FROM $parent.refs)
		END AS resolved_refs,
		refs AS raw_refs
	FROM i7154 WHERE active;

CREATE u7154:s1 SET name = 'Alice';
CREATE u7154:s2 SET name = 'Bob';

CREATE i7154:i1 SET refs = [u7154:s1, u7154:s2], active = true;
CREATE i7154:i2 SET refs = NONE, active = true;

SELECT * FROM v7154_a ORDER BY id;

SELECT id,
	IF type::is_none(refs) THEN NONE
	ELSE (SELECT VALUE { id: $this.id } FROM $parent.refs)
	END AS resolved_refs
FROM i7154 WHERE active
ORDER BY id;
