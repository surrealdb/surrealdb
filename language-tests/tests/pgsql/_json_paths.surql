/**
[test]
run = false
reason = "Fixture for json_paths.pgsql: documents whose keys spell what a text-array path treats specially, and nesting deep enough that a path takes every form of a step"
*/

DEFINE TABLE jpath SCHEMALESS;

CREATE jpath:1 SET name = 'keys', data = { 'NULL': 'null-key', 'a\\b': 'backslash-key', 'a,b': 'comma-key', a: { b: 'ab' }, nk: { 'NULL': 'null-below' } } RETURN NONE;
CREATE jpath:2 SET name = 'deep', data = [0, [0, [0, [0, [0, ['x', 'y']]]]]] RETURN NONE;
CREATE jpath:3 SET name = 'minus', data = { '-1': { '-1': 'inner', '01': 'padded' }, list: [{ '-1': 'in-list' }] } RETURN NONE;
CREATE jpath:4 SET name = 'text', data = 'hello' RETURN NONE;
CREATE jpath:5 SET name = 'number', data = 42 RETURN NONE;
