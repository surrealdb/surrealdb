/**
[test]
reason = """
	Setup for id_components_permissions: a table whose select permission reads
	a component of its record id, a table whose stored field has a select
	permission that reads one, and the record user the test runs as. Runs as
	owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:london SET site = 'London' RETURN NONE;

-- The table permission reads a component: a record user sees the readings of
-- their own city.
DEFINE TABLE reading SCHEMAFULL PERMISSIONS FOR select WHERE city = $auth.site;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading TYPE int;
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 4 RETURN NONE;
CREATE reading:['London', d'2024-01-02T00:00:00Z'] SET temp = 5 RETURN NONE;
CREATE reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 6 RETURN NONE;
CREATE reading:['Paris', d'2024-01-03T00:00:00Z'] SET temp = 7 RETURN NONE;

-- The permission of a stored field reads a component: a record user sees the
-- level of their own site's gauge.
DEFINE TABLE gauge SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD id ON gauge TYPE [site: string, seq: int];
DEFINE FIELD level ON gauge TYPE int PERMISSIONS FOR select WHERE site = $auth.site;
CREATE gauge:['London', 1] SET level = 10 RETURN NONE;
CREATE gauge:['Paris', 1] SET level = 20 RETURN NONE;
