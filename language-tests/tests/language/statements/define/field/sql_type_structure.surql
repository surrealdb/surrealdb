/**
[env]
namespace = true
database = true

[test]
reason = "INFO FOR TABLE ... STRUCTURE reports a declared SQL type as its dialect and text, and reports nothing for a field without one"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ kind: 'int', name: 'm', permissions: { create: true, select: true, update: true }, readonly: false, table: 't' }, { kind: 'decimal', name: 'n', permissions: { create: true, select: true, update: true }, readonly: false, sql_type: { dialect: 'POSTGRESQL', text: 'numeric(10,2)' }, table: 't' }]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE decimal SQL TYPE POSTGRESQL 'numeric(10,2)';
DEFINE FIELD m ON t TYPE int;
-- The structure form of each field, the declared type as its dialect and text.
RETURN (INFO FOR TABLE t STRUCTURE).fields;
