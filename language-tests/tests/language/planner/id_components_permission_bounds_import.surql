/**
[test]
reason = """
	Setup for id_components_permission_bounds and
	id_components_permission_bounds_for_another_user: a table keyed by a city
	and a datetime whose `PERMISSIONS FOR select` admits the rows of the
	caller's own city, and two record users, `user:alice` of London and
	`user:bob` of Paris. London holds temps 1 to 3, Paris 4 and 5.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:alice SET city = 'London';
CREATE user:bob SET city = 'Paris';

DEFINE TABLE reading SCHEMALESS PERMISSIONS FOR select WHERE city = $auth.city;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1;
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:01.500Z', temp = 2;
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:03Z', temp = 3;
CREATE reading SET city = 'Paris', time = d'2024-01-01T00:00:00Z', temp = 4;
CREATE reading SET city = 'Paris', time = d'2024-01-01T00:00:02Z', temp = 5;
