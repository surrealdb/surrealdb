/**
[test]
reason = "ALTER FIELD sets and drops the declared SQL type with SQL TYPE and DROP SQL TYPE, alongside the other clauses; the definition in INFO follows each change"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { n: "DEFINE FIELD n ON t TYPE decimal SQL TYPE POSTGRESQL 'numeric(12,4)' PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { n: 'DEFINE FIELD n ON t TYPE decimal PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { n: "DEFINE FIELD n ON t TYPE int SQL TYPE MYSQL 'BIGINT UNSIGNED' PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE TABLE t;
DEFINE FIELD n ON t TYPE decimal SQL TYPE POSTGRESQL 'numeric(10,2)';
ALTER FIELD n ON t SQL TYPE POSTGRESQL 'numeric(12,4)';
INFO FOR TABLE t;
ALTER FIELD n ON t DROP SQL TYPE;
INFO FOR TABLE t;
ALTER FIELD IF EXISTS n ON t TYPE int SQL TYPE MYSQL 'BIGINT UNSIGNED' DROP COMMENT;
INFO FOR TABLE t;
REMOVE FIELD n ON t;
INFO FOR TABLE t;
