/**
[test]
reason = """
	Setup for a_permission_chain_partitions_by_key_range: a table whose
	`PERMISSIONS FOR select` admits a row on a row-dependent predicate, a
	computed field every caller may read, a computed field guarded by it and
	a stored field guarded by it, and the record user the test runs as. The
	six rows split three ways under the permission chain -- dropped by the
	table permission, admitted with both guarded fields cut, admitted with
	both kept -- and their record ids are consecutive integers, so a
	half-open range over the id partitions them exactly.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:alice;
CREATE user:bob;

DEFINE TABLE doc SCHEMAFULL PERMISSIONS FOR select WHERE public = true OR owner = $auth.id;
DEFINE FIELD owner ON doc TYPE record<user>;
DEFINE FIELD public ON doc TYPE bool;
DEFINE FIELD n ON doc TYPE int;
-- Readable by everyone; both guards below read it.
DEFINE FIELD is_mine ON doc TYPE bool COMPUTED owner = $auth.id;
-- Derived from a stored field, guarded by the computed field above.
DEFINE FIELD double ON doc TYPE int COMPUTED n * 2 PERMISSIONS FOR select WHERE is_mine;
-- Stored, guarded by the same computed field.
DEFINE FIELD secret ON doc TYPE string PERMISSIONS FOR select WHERE is_mine;

CREATE doc:1 SET owner = user:alice, public = true,  n = 1, secret = 's1';
CREATE doc:2 SET owner = user:bob,   public = true,  n = 2, secret = 's2';
CREATE doc:3 SET owner = user:bob,   public = false, n = 3, secret = 's3';
CREATE doc:4 SET owner = user:alice, public = false, n = 4, secret = 's4';
CREATE doc:5 SET owner = user:bob,   public = true,  n = 5, secret = 's5';
CREATE doc:6 SET owner = user:alice, public = true,  n = 6, secret = 's6';
