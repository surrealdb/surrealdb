/**
[test]
reason = "LIGHTWEIGHT relations: grammar in either flag order, ENFORCED implied and rendered canonically, INFO round-trip, and the definition-shape rejections (missing IN/OUT, record-consuming clauses, schema objects, clearing the flag)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { follows: 'DEFINE TABLE follows TYPE RELATION IN person OUT person ENFORCED LIGHTWEIGHT SCHEMALESS PERMISSIONS NONE', liked: 'DEFINE TABLE liked TYPE RELATION IN person OUT person ENFORCED LIGHTWEIGHT SCHEMALESS PERMISSIONS NONE', person: 'DEFINE TABLE person TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation requires explicit IN and OUT tables: the record-less scan enumerates its edges from the IN tables' adjacency"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation requires explicit IN and OUT tables: the record-less scan enumerates its edges from the IN tables' adjacency"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation cannot be declared SCHEMAFULL: its edges store no records"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation cannot be declared CHANGEFEED: its edges store no records"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation cannot be declared DROP: its edges store no records"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation cannot carry a field definition: its edges store no records"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation cannot carry an index: its edges store no records"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation cannot carry an event: its edges store no records"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { in: 'DEFINE FIELD in ON follows TYPE record<person | other> PERMISSIONS FULL', out: 'DEFINE FIELD out ON follows TYPE record<person> PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

-- Both flag orders parse; INFO renders the canonical `ENFORCED LIGHTWEIGHT`.
DEFINE TABLE person;
DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
DEFINE TABLE liked TYPE RELATION IN person OUT person LIGHTWEIGHT ENFORCED;
INFO FOR DB;

-- IN and OUT are required.
DEFINE TABLE bad TYPE RELATION LIGHTWEIGHT;
DEFINE TABLE bad TYPE RELATION IN person LIGHTWEIGHT;

-- Record-consuming clauses are rejected.
DEFINE TABLE bad TYPE RELATION IN person OUT person LIGHTWEIGHT SCHEMAFULL;
DEFINE TABLE bad TYPE RELATION IN person OUT person LIGHTWEIGHT CHANGEFEED 1h;
DEFINE TABLE bad TYPE RELATION IN person OUT person LIGHTWEIGHT DROP;

-- Schema objects are rejected: the edges store no records to apply them to.
DEFINE FIELD weight ON follows TYPE number;
DEFINE INDEX idx ON follows FIELDS in;
DEFINE EVENT ev ON follows WHEN true THEN {};

-- The flag can never be cleared; IN/OUT may only grow.
DEFINE TABLE OVERWRITE follows TYPE RELATION IN person OUT person;
DEFINE TABLE other;
DEFINE TABLE OVERWRITE follows TYPE RELATION IN person | other OUT person LIGHTWEIGHT;
INFO FOR TABLE follows;
