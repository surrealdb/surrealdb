/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "Bare field name `parent` (object.sub) must not be treated as $parent correlation — needed-fields / selective scan (#7154 review)"
issue = 7154

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u7154_parent_field:r1, parent: { sub: 'ok' } }]"

[[test.results]]
value = "[{ parent: { sub: 'ok' } }]"

*/

DEFINE TABLE OVERWRITE u7154_parent_field TYPE NORMAL SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD OVERWRITE parent ON u7154_parent_field TYPE object PERMISSIONS FULL;
DEFINE FIELD OVERWRITE parent.sub ON u7154_parent_field TYPE string PERMISSIONS FULL;

CREATE u7154_parent_field:r1 SET parent = { sub: 'ok' };

SELECT parent.sub FROM u7154_parent_field;
