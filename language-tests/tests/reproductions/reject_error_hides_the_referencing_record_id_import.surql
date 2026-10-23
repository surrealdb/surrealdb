/**
[test]
reason = """
	Setup for reject_error_hides_the_referencing_record_id. Two referencing
	shapes a record user may trip over, both signed in through one record access:

	Direct: an `asset` the user may delete, and a `report` whose rows are
	SELECT-restricted (only `public = true` is visible) and reference an asset ON
	DELETE REJECT, with one hidden referencing row.

	Cascaded: a `project` the user may delete, a hidden `note` that references the
	project ON DELETE CASCADE, and a hidden `lock` that references the note ON
	DELETE REJECT. Deleting the project cascades into the note, whose own delete
	the lock rejects, so the rejection surfaces from a cascade frame that
	suppresses enforcement.
"""
run = false
*/

DEFINE TABLE asset SCHEMALESS PERMISSIONS FULL;

DEFINE TABLE report SCHEMALESS PERMISSIONS
	FOR select WHERE public = true,
	FOR create, update, delete NONE;
DEFINE FIELD about ON report TYPE record<asset> REFERENCE ON DELETE REJECT;
DEFINE FIELD public ON report TYPE bool;

DEFINE TABLE project SCHEMALESS PERMISSIONS FULL;

DEFINE TABLE note SCHEMALESS PERMISSIONS
	FOR select, create, update, delete NONE;
DEFINE FIELD parent ON note TYPE record<project> REFERENCE ON DELETE CASCADE;

DEFINE TABLE lock SCHEMALESS PERMISSIONS
	FOR select, create, update, delete NONE;
DEFINE FIELD target ON lock TYPE record<note> REFERENCE ON DELETE REJECT;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:alice;

CREATE asset:secret;
CREATE report:hidden SET about = asset:secret, public = false;

CREATE project:visible;
CREATE note:hidden SET parent = project:visible;
CREATE lock:hidden SET target = note:hidden;
