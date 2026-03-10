/**
[test]
reason = "Import: setup tables with different permissions for range count tests"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bar: 'hello', foo: 'world', id: table_none:me }]"

[[test.results]]
value = """[{ bar: "don't", foo: 'show up', id: table_none:you }]"""

[[test.results]]
value = "[{ bar: 'hello', foo: 'world', id: table_full:me }]"

[[test.results]]
value = """[{ bar: "don't", foo: 'show up', id: table_full:you }]"""

[[test.results]]
value = "[{ bar: 'hello', foo: 'world', id: table_where_false:me }]"

[[test.results]]
value = """[{ bar: "don't", foo: 'show up', id: table_where_false:you }]"""

[[test.results]]
value = "[{ bar: 'hello', foo: 'world', id: table_where_match:me }]"

[[test.results]]
value = """[{ bar: "don't", foo: 'show up', id: table_where_match:you }]"""
*/

DEFINE TABLE table_none PERMISSIONS FOR SELECT NONE;
DEFINE TABLE table_full PERMISSIONS FOR SELECT FULL;
DEFINE TABLE table_where_false PERMISSIONS FOR SELECT WHERE FALSE;
DEFINE TABLE table_where_match PERMISSIONS FOR SELECT WHERE bar = 'hello';
CREATE table_none:me CONTENT { bar: 'hello', foo: 'world' };
CREATE table_none:you CONTENT { bar: "don't", foo: 'show up' };
CREATE table_full:me CONTENT { bar: 'hello', foo: 'world' };
CREATE table_full:you CONTENT { bar: "don't", foo: 'show up' };
CREATE table_where_false:me CONTENT { bar: 'hello', foo: 'world' };
CREATE table_where_false:you CONTENT { bar: "don't", foo: 'show up' };
CREATE table_where_match:me CONTENT { bar: 'hello', foo: 'world' };
CREATE table_where_match:you CONTENT { bar: "don't", foo: 'show up' };
