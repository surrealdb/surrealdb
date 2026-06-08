/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:alice }, { id: person:bob }, { id: person:dave }]"

[[test.results]]
value = "[{ id: friend:f_hidden, in: person:bob, out: person:dave }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select FULL;

-- At the historical timestamp, the edge has a row-level SELECT gate:
-- a record user may only see edges whose IN endpoint is their own auth
-- record. The `try_fast_path_pair` planner used to check the current
-- catalog only, so once the table was overwritten to `select FULL`
-- below, a `VERSION` query took the TargetVertex fast path and skipped
-- this historical WHERE entirely.
DEFINE TABLE friend TYPE RELATION SCHEMALESS
    PERMISSIONS
        FOR select WHERE in.id = $auth.id
        FOR create FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

CREATE person:alice, person:bob, person:dave;
RELATE person:bob -> friend:f_hidden -> person:dave;

SLEEP 50ms;

-- Capture the historical timestamp via a DB-level param. The scan
-- operators now read `version_stamp` from the enclosing `VersionScope`
-- rather than re-evaluating `VERSION $hist` under the just-set stamp,
-- so the param's storage revision being after `time::now()` no longer
-- breaks `... VERSION $hist`.
DEFINE PARAM $hist VALUE time::now() PERMISSIONS FULL;

SLEEP 50ms;

-- Overwrite the current catalog to `SELECT FULL`, which is what the
-- vulnerable fast-path eligibility check looked at.
DEFINE TABLE OVERWRITE friend TYPE RELATION SCHEMALESS
    PERMISSIONS
        FOR select FULL
        FOR create FULL;
