/**
[test]
reason = """
	Setup for the id_components_* tests of this directory: a SCHEMAFULL table
	whose record id names its components, four readings across two cities and
	three days, and a computed field that reads a component. Runs as owner.
"""
run = false
*/

DEFINE TABLE reading SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading TYPE int;
-- A computed field that reads a component, which derives before it.
DEFINE FIELD tag ON reading TYPE string COMPUTED string::concat(city, '-', temp);

CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 4 RETURN NONE;
CREATE reading:['London', d'2024-01-02T00:00:00Z'] SET temp = 5 RETURN NONE;
CREATE reading:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 6 RETURN NONE;
CREATE reading:['Paris', d'2024-01-03T00:00:00Z'] SET temp = 7 RETURN NONE;
