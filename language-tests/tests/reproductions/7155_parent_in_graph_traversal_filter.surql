/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "$parent with multi-level field access should resolve inside graph traversal WHERE conditions"
issue = 7155

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tag:man, name: 'man' }]"

[[test.results]]
value = "[{ id: tag:woman, name: 'woman' }]"

[[test.results]]
value = "[{ active: true, id: contact:c1 }]"

[[test.results]]
value = "[{ active: true, id: moment:m1, visibility: { tags: ['man'] } }]"

[[test.results]]
value = "[{ id: visible:x, in: moment:m1, out: tag:man }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: tagged:x, in: contact:c1, out: tag:man }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: moment:m1, matched_tags: [tag:man] }]"

[[test.results]]
value = "[{ contacts: [contact:c1], id: moment:m1 }]"

[[test.results]]
value = "[{ filtered_edges: [visible:x], id: moment:m1 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ hardcoded_tags: [tag:man], id: moment:m1 }]"

*/

DEFINE TABLE OVERWRITE moment SCHEMAFULL TYPE NORMAL;
DEFINE FIELD OVERWRITE visibility ON moment TYPE object;
DEFINE FIELD OVERWRITE visibility.tags ON moment TYPE array<string>;
DEFINE FIELD OVERWRITE active ON moment TYPE bool DEFAULT true;

DEFINE TABLE OVERWRITE tag SCHEMAFULL TYPE NORMAL;
DEFINE FIELD OVERWRITE name ON tag TYPE string;

DEFINE TABLE OVERWRITE contact SCHEMAFULL TYPE NORMAL;
DEFINE FIELD OVERWRITE active ON contact TYPE bool DEFAULT true;

DEFINE TABLE OVERWRITE visible SCHEMAFULL TYPE RELATION IN moment OUT tag;
DEFINE TABLE OVERWRITE tagged SCHEMAFULL TYPE RELATION IN contact OUT tag;

CREATE tag:man SET name = 'man';
CREATE tag:woman SET name = 'woman';
CREATE contact:c1 SET active = true;
CREATE moment:m1 SET visibility = { tags: ['man'] }, active = true;
RELATE moment:m1->visible->tag:man;
RELATE contact:c1->tagged->tag:man;

-- $parent.visibility.tags (multi-level field access) inside graph traversal filter
SELECT
  id,
  id->visible->(tag WHERE name IN $parent.visibility.tags) as matched_tags
FROM moment WHERE id = moment:m1;

-- Full chain: moment -> visible -> tag (filtered) <- tagged <- contact (filtered)
SELECT
  id,
  id->visible->(tag WHERE name IN $parent.visibility.tags)<-tagged<-(contact WHERE active) as contacts
FROM moment WHERE id = moment:m1;

-- $parent in graph edge WHERE with multi-level field access
SELECT
  id,
  ->visible[WHERE out.name IN $parent.visibility.tags] as filtered_edges
FROM moment WHERE id = moment:m1;

-- Control: hardcoded value works
SELECT
  id,
  id->visible->(tag WHERE name IN ['man']) as hardcoded_tags
FROM moment WHERE id = moment:m1;
