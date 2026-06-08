/**
[test]
reason = "Setup: a `task` table whose SELECT permission traverses two edge tables (`rel_workspace_journey`, `rel_user_member_of_workspace`) that have NO explicit permissions (default NONE). Node tables are readable so the variable under test is purely the edge tables crossed by the permission predicate. Mirrors a user report after the 3.1.x edge-permission security fix: a `task` SELECT permission that walks edges stopped matching once edge tables defaulted to NONE."
run = false
*/

-- Node tables are readable; the question under test is purely about the
-- EDGE tables crossed by the permission predicate.
DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE TABLE workspace SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE TABLE journey SCHEMALESS PERMISSIONS FOR select FULL;

-- `task` is readable only by a member of the workspace that owns its
-- journey. The WHERE clause traverses the two edge tables defined below.
DEFINE TABLE task SCHEMAFULL
	PERMISSIONS FOR select
		WHERE $auth.id IN journey<-rel_workspace_journey<-workspace<-rel_user_member_of_workspace<-user;
DEFINE FIELD journey ON task TYPE record<journey>;

-- Edge tables WITHOUT any PERMISSIONS clause -> default NONE.
DEFINE TABLE rel_workspace_journey TYPE RELATION SCHEMALESS;
DEFINE TABLE rel_user_member_of_workspace TYPE RELATION SCHEMALESS;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:martin, user:other;
CREATE workspace:w1;
CREATE journey:j1;
CREATE task:t1 SET journey = journey:j1;

-- martin is a member of w1; w1 owns journey j1; task t1 belongs to j1.
RELATE user:martin -> rel_user_member_of_workspace -> workspace:w1;
RELATE workspace:w1 -> rel_workspace_journey -> journey:j1;
