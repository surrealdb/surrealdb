/**
[test]
run = false
reason = "Fixture for json_operators.pgsql: one row per JSON shape a document column can hold, so each operator is checked against an array, an object, a scalar, a JSON null and an absent column"
*/

DEFINE TABLE jdoc SCHEMALESS;

CREATE jdoc:1 SET name = 'arr_mixed', data = [1, 'two', NULL, { x: 1 }], key = 'x', idx = 1 RETURN NONE;
CREATE jdoc:2 SET name = 'arr_nested', data = [[1, 2], [3, [4, 5]], { x: 'in-array' }] RETURN NONE;
CREATE jdoc:3 SET name = 'obj_arr', data = { arr: [{ x: 1 }, { x: 2 }] } RETURN NONE;
CREATE jdoc:4 SET name = 'obj_deep', data = { e: { f: [10, 20, 30] } } RETURN NONE;
CREATE jdoc:5 SET name = 'obj_keys', data = { '0': 'zero', '1': 'one', '-1': 'minus', '01': 'oh-one', x: 'ex' } RETURN NONE;
CREATE jdoc:6 SET name = 'scalar_str', data = 'hello' RETURN NONE;
CREATE jdoc:7 SET name = 'scalar_num', data = 42 RETURN NONE;
CREATE jdoc:8 SET name = 'scalar_bool', data = true RETURN NONE;
CREATE jdoc:9 SET name = 'scalar_null', data = NULL RETURN NONE;
CREATE jdoc:10 SET name = 'absent' RETURN NONE;
CREATE jdoc:11 SET name = 'obj_flat', data = { a: 1, b: 'text', c: true, d: false, e: 0, f: NULL, g: { h: 1 }, i: [1, 2], x: 'flat-x' }, key = 'x', idx = 1 RETURN NONE;
CREATE jdoc:12 SET name = 'obj_nest', data = { l1: { l2: { l3: { l4: { l5: ['bottom'] } } } } } RETURN NONE;
