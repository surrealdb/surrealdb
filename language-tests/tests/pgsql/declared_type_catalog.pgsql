/**
[env]
namespace = true
database = true
imports = ["pgsql/_sized_columns.surql"]

[test]
reason = "A column declared with a sized PostgreSQL type is reported by information_schema.columns with its length, precision, scale and datetime precision, by pg_attribute with the typmod of that type, and by format_type with the modifier"

[[test.results]]
value = "[{ character_maximum_length: 5, character_octet_length: 20, column_name: 'c', data_type: 'character', datetime_precision: NULL, numeric_precision: NULL, numeric_precision_radix: NULL, numeric_scale: NULL, udt_name: 'bpchar' }, { character_maximum_length: NULL, character_octet_length: NULL, column_name: 'n', data_type: 'numeric', datetime_precision: NULL, numeric_precision: 10, numeric_precision_radix: 10, numeric_scale: 2, udt_name: 'numeric' }, { character_maximum_length: NULL, character_octet_length: NULL, column_name: 'ts', data_type: 'timestamp without time zone', datetime_precision: 3, numeric_precision: NULL, numeric_precision_radix: NULL, numeric_scale: NULL, udt_name: 'timestamp' }, { character_maximum_length: 20, character_octet_length: 80, column_name: 'v', data_type: 'character varying', datetime_precision: NULL, numeric_precision: NULL, numeric_precision_radix: NULL, numeric_scale: NULL, udt_name: 'varchar' }]"

[[test.results]]
value = "[{ attname: 'c', atttypid: 1042, atttypmod: 9, formatted: 'character(5)' }, { attname: 'id', atttypid: 25, atttypmod: -1, formatted: 'text' }, { attname: 'n', atttypid: 1700, atttypmod: 655366, formatted: 'numeric(10,2)' }, { attname: 'ts', atttypid: 1114, atttypmod: 3, formatted: 'timestamp(3) without time zone' }, { attname: 'v', atttypid: 1043, atttypmod: 24, formatted: 'character varying(20)' }]"

[[test.results]]
value = "[{ c: 'character(5)', i: 'integer', n: 'numeric(10,2)', ts: 'timestamp(3) without time zone', v: 'character varying(20)' }]"
*/

SELECT column_name, data_type, udt_name, character_maximum_length, character_octet_length, numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision FROM information_schema.columns WHERE table_name = 'sized' AND column_name <> 'id' ORDER BY column_name;

SELECT a.attname, a.atttypid, a.atttypmod, format_type(a.atttypid, a.atttypmod) AS formatted FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'sized' AND a.attnum > 0 ORDER BY a.attname;

SELECT format_type(1043, 24) AS v, format_type(1042, 9) AS c, format_type(1700, 655366) AS n, format_type(1114, 3) AS ts, format_type(23, -1) AS i;
