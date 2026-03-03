/**
[test]
reason = "Multi-column unique indexes should ignore NONE/NULL values, matching SQL-standard semantics where NULLs are never considered equal for uniqueness"
issue = 3290

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f1: '2', id: t1:vb8vajoueg055hffmaon }]"
skip-record-id-key = true

[[test.results]]
value = "[{ f1: '2', id: t1:iiwjbfgw36d3qtmx3l12 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ f1: 'a', f2: 'b', id: t1:kpbyjsraxhbrh30yciwt }]"
skip-record-id-key = true

[[test.results]]
match = "$error = /Database index `idx_t1_f1_f2` already contains \\['a', 'b'\\]/"
error = true

*/
DEFINE TABLE t1 SCHEMAFULL;
DEFINE FIELD f1 ON TABLE t1 TYPE string;
DEFINE FIELD f2 ON TABLE t1 TYPE option<string>;
DEFINE INDEX idx_t1_f1_f2 ON TABLE t1 COLUMNS f1, f2 UNIQUE;

-- Both rows have f2 = NONE; should not conflict
CREATE t1 SET f1 = '2';
CREATE t1 SET f1 = '2';

-- Rows with explicit non-null values in all columns should still conflict
CREATE t1 SET f1 = 'a', f2 = 'b';
CREATE t1 SET f1 = 'a', f2 = 'b';
