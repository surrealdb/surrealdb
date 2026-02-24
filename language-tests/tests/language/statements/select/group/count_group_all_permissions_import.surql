/**
[test]
reason = "Import: setup tables with different permissions for GROUP ALL count tests"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bar: 'hello', foo: 'world', id: table_none:baz }]"

[[test.results]]
value = "[{ bar: 'hello', foo: 'world', id: table_full:baz }]"

[[test.results]]
value = "[{ bar: 'hello', foo: 'world', id: table_where_false:baz }]"
*/

DEFINE TABLE table_none PERMISSIONS FOR SELECT NONE;
DEFINE TABLE table_full PERMISSIONS FOR SELECT FULL;
DEFINE TABLE table_where_false PERMISSIONS FOR SELECT WHERE FALSE;
CREATE table_none:baz CONTENT { bar: 'hello', foo: 'world' };
CREATE table_full:baz CONTENT { bar: 'hello', foo: 'world' };
CREATE table_where_false:baz CONTENT { bar: 'hello', foo: 'world' };
