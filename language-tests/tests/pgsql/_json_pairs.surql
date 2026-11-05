/**
[test]
run = false
reason = "Fixture for json_path_over_a_changing_value.pgsql: rows that each hold an object and an array that one path reaches a value in, so a path over a value that picks one of the two reaches a value whichever it picks"
*/

DEFINE TABLE jpair SCHEMALESS;

FOR $i IN 0..200 {
	CREATE jpair SET n = $i, obj = { '-1': 'from-object' }, arr = ['from-array'], obj3 = { '-1': { '-1': { '-1': 'from-object' } } }, arr3 = [[['from-array']]] RETURN NONE;
};
