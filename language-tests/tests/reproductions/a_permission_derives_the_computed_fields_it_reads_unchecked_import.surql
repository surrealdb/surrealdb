/**
[test]
reason = "Setup for a_permission_derives_the_computed_fields_it_reads_unchecked.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: meta:secret, secret: true }]"

[[test.results]]
value = "[{ id: meta:open, secret: false }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hidden: true, id: note:secret, lock: meta:open, locked: false, meta: meta:secret }]"

[[test.results]]
value = "[{ hidden: false, id: note:open, lock: meta:secret, locked: true, meta: meta:open }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ring:1, members: 1 }]"

[[test.results]]
value = "[{ id: ring:2, members: 2 }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Records the caller may not select.
DEFINE TABLE meta SCHEMALESS PERMISSIONS NONE;
CREATE meta:secret SET secret = true;
CREATE meta:open SET secret = false;

-- Deny-intended through a record the caller may not select. Derived as the
-- caller, `meta.secret` is `NONE`, so `hidden != true` admits every row and
-- `locked != true` every update.
DEFINE TABLE note SCHEMALESS
    PERMISSIONS FOR select WHERE hidden != true, FOR create FULL, FOR update WHERE locked != true;
DEFINE FIELD hidden ON note COMPUTED meta.secret;
DEFINE FIELD locked ON note COMPUTED lock.secret;

CREATE note:secret SET meta = meta:secret, lock = meta:open;
CREATE note:open SET meta = meta:open, lock = meta:secret;

-- A computed field that reads its own table, read by that table's
-- permission. The subquery derives no computed field of its own; derived as
-- the caller, it is judged by the same permission, which derives the field
-- again, without end.
DEFINE TABLE ring SCHEMALESS PERMISSIONS FOR select WHERE members > 1, FOR create FULL;
DEFINE FIELD members ON ring COMPUTED count(SELECT VALUE id FROM ring);

CREATE ring:1;
CREATE ring:2;
