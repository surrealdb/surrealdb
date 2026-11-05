/**
[test]
run = false
*/

DEFINE TABLE doc SCHEMALESS;

CREATE doc:1 SET name = 'a', data = { city: 'London', n: 3, ok: true, tags: ['x', 'y'], geo: { lat: 51.5 }, gone: NULL } RETURN NONE;
CREATE doc:2 SET name = 'b', data = { city: 'Paris' } RETURN NONE;
CREATE doc:3 SET name = 'c' RETURN NONE;
