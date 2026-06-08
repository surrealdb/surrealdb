/**
[test]
reason = "Import: tables that grant write but cut select, used to verify post-write SELECT permission filtering on RETURN AFTER"
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

DEFINE TABLE actors SCHEMALESS PERMISSIONS FULL;
CREATE actors:writer;

-- Table-level: writer can CREATE / UPDATE / DELETE but cannot SELECT.
-- The post-write SELECT permission cut must apply to RETURN AFTER /
-- default-RETURN, mirroring what a regular SELECT would return.
DEFINE TABLE notes SCHEMALESS PERMISSIONS FOR create, update, delete FULL, FOR select NONE;

-- Field-level: writer can write `body` but a regular SELECT would cut
-- it. The post-write apply_select_field_permissions pass must produce
-- the same cut on RETURN AFTER output.
DEFINE TABLE entries SCHEMAFULL PERMISSIONS FOR create, update, delete FULL, FOR select FULL;

DEFINE FIELD body ON entries TYPE string PERMISSIONS FOR create, update FULL, FOR select NONE;

DEFINE FIELD label ON entries TYPE string PERMISSIONS FULL;

-- Existing actors to use as RELATE endpoints in the test.
CREATE actors:a;
CREATE actors:b;

-- A relation table with FOR select NONE so the write-path cut applies
-- to RELATE (which goes through relate_create / relate_update like
-- create.rs / upsert.rs).
DEFINE TABLE links TYPE RELATION SCHEMALESS PERMISSIONS FOR create, update, delete FULL, FOR select NONE;
