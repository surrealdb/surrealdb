/**
[test]
reason = """
	Setup for a_range_over_a_composite_unique_index_prefix_bounds_the_whole_prefix
	and its _plan companion: two tables holding the same rows, `tu` with a UNIQUE
	index over (g, a) and `tc` with a plain one. Three rows share g = 'y', one of
	them with no `a`, which a UNIQUE index stores as a plain entry (a tuple holding
	NONE collides with no other). Runs as owner.
"""
run = false
*/

DEFINE TABLE tu SCHEMAFULL;
DEFINE FIELD g ON tu TYPE string;
DEFINE FIELD a ON tu TYPE option<int>;
CREATE tu:1 SET g = 'x', a = 5;
CREATE tu:2 SET g = 'y', a = 5;
CREATE tu:3 SET g = 'y', a = 6;
CREATE tu:4 SET g = 'y';
CREATE tu:5 SET g = 'z', a = 1;
DEFINE INDEX uu ON tu FIELDS g, a UNIQUE;

DEFINE TABLE tc SCHEMAFULL;
DEFINE FIELD g ON tc TYPE string;
DEFINE FIELD a ON tc TYPE option<int>;
CREATE tc:1 SET g = 'x', a = 5;
CREATE tc:2 SET g = 'y', a = 5;
CREATE tc:3 SET g = 'y', a = 6;
CREATE tc:4 SET g = 'y';
CREATE tc:5 SET g = 'z', a = 1;
DEFINE INDEX ug ON tc FIELDS g, a;
