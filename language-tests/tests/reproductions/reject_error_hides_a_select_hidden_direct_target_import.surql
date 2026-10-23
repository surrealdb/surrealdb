/**
[test]
reason = """
	Setup for reject_error_hides_a_select_hidden_direct_target. A `vault` the
	record user may delete but not select (FOR delete FULL, FOR select NONE), and
	a hidden `holder` that references a vault ON DELETE REJECT. A record access
	signs the user in. A bare `DELETE vault` reaches `vault:1` through the delete
	permission though the user cannot SELECT it, so a rejection naming it would
	disclose a row across the SELECT boundary.
"""
run = false
*/

DEFINE TABLE vault SCHEMALESS PERMISSIONS
	FOR select NONE,
	FOR create, update, delete FULL;

DEFINE TABLE holder SCHEMALESS PERMISSIONS
	FOR select, create, update, delete NONE;
DEFINE FIELD held ON holder TYPE record<vault> REFERENCE ON DELETE REJECT;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:alice;
CREATE vault:1;
CREATE holder:1 SET held = vault:1;
