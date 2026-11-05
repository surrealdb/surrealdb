/**
[test]
reason = "Setup for a_write_permission_computes_fields_in_dependency_order.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ can_edit: false, id: t:locked, is_locked: true, status: 'locked' }]"

[[test.results]]
value = "[{ can_edit: true, id: t:open, is_locked: false, status: 'open' }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- `can_edit` reads `is_locked`, which sorts after it by name. Computed in name
-- order, `can_edit` would be derived from `is_locked = NONE`, and `!NONE` is
-- true.
DEFINE TABLE t SCHEMALESS
    PERMISSIONS FOR select FULL, FOR create, update, delete WHERE can_edit = true;
DEFINE FIELD is_locked ON t COMPUTED status = 'locked';
DEFINE FIELD can_edit ON t COMPUTED !is_locked;

CREATE t:locked SET status = 'locked';
CREATE t:open SET status = 'open';
