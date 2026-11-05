/**
[test]
reason = "Setup for a_write_permission_reads_quoted_computed_fields.surql."

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
value = """[{ can_edit: false, id: t:locked, "is-locked": true, status: 'locked' }]"""

[[test.results]]
value = """[{ can_edit: true, id: t:open, "is-locked": false, status: 'open' }]"""

[[test.results]]
value = """[{ id: u:locked, status: 'locked', "verrouillé": true }]"""

[[test.results]]
value = """[{ id: u:open, status: 'open', "verrouillé": false }]"""

[[test.results]]
value = """[{ "can-edit": false, id: v:locked, "is-locked": true, status: 'locked' }]"""

[[test.results]]
value = """[{ "can-edit": true, id: v:open, "is-locked": false, status: 'open' }]"""

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- `can_edit` reads `is-locked`, a name that has to be quoted and that sorts
-- after `can_edit`. A read of it records the bare identifier `is-locked`, so the
-- gate has to know the field by that name to derive it, and to derive it first.
DEFINE TABLE t SCHEMALESS
    PERMISSIONS FOR select FULL, FOR create, update, delete WHERE can_edit = true;
DEFINE FIELD `is-locked` ON t COMPUTED status = 'locked';
DEFINE FIELD can_edit ON t COMPUTED !`is-locked`;

-- The predicate reads a quoted computed field itself, and is written to deny:
-- judged against NONE, `NONE != true` admits every write.
DEFINE TABLE u SCHEMALESS
    PERMISSIONS FOR select FULL, FOR create, update, delete WHERE `verrouillé` != true;
DEFINE FIELD `verrouillé` ON u COMPUTED status = 'locked';

-- Both fields quoted, so the field reading `is-locked` comes before it in the
-- table's field list, behind a predicate that reads a parameter: the gate
-- derives every computed field, and only the order is at stake.
DEFINE TABLE v SCHEMALESS
    PERMISSIONS FOR select FULL, FOR create, update, delete WHERE `can-edit` = true AND $auth != NONE;
DEFINE FIELD `is-locked` ON v COMPUTED status = 'locked';
DEFINE FIELD `can-edit` ON v COMPUTED !`is-locked`;

CREATE t:locked SET status = 'locked';
CREATE t:open SET status = 'open';
CREATE u:locked SET status = 'locked';
CREATE u:open SET status = 'open';
CREATE v:locked SET status = 'locked';
CREATE v:open SET status = 'open';
