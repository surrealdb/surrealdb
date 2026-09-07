/**
[test]
reason = "INLINE fields: DEFINE and ALTER grammar, INFO rendering only when set, and the definition-shape rejections (non-relation tables, the id/in/out synthetics, nested paths, COMPUTED fields, lightweight relations, and demoting a relation that still has INLINE fields)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { in: 'DEFINE FIELD in ON likes TYPE record PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record PERMISSIONS FULL', score: 'DEFINE FIELD score ON likes TYPE number INLINE PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { in: 'DEFINE FIELD in ON likes TYPE record PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record PERMISSIONS FULL', score: 'DEFINE FIELD score ON likes TYPE number PERMISSIONS FULL', tag: 'DEFINE FIELD tag ON likes TYPE none | string INLINE PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
error = "An error occurred: INLINE requires an existing TYPE RELATION table: the value is embedded into the edges' adjacency payloads"

[[test.results]]
error = "An error occurred: INLINE requires an existing TYPE RELATION table: the value is embedded into the edges' adjacency payloads"

[[test.results]]
error = "An error occurred: the `id`, `in` and `out` fields already live in the adjacency keys and cannot be INLINE"

[[test.results]]
error = "An error occurred: only a top-level field can be INLINE"

[[test.results]]
error = "An error occurred: a COMPUTED field cannot be INLINE: its value is derived at read time, so there is nothing stored to embed"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation cannot carry a field definition: its edges store no records"

[[test.results]]
error = "An error occurred: the relation `likes` still has INLINE fields; remove or un-inline them before changing its type"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { in: 'DEFINE FIELD in ON likes TYPE record PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record PERMISSIONS FULL', score: 'DEFINE FIELD score ON likes TYPE number PERMISSIONS FULL', tag: 'DEFINE FIELD tag ON likes TYPE none | string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE TABLE person;
DEFINE TABLE likes TYPE RELATION;
DEFINE FIELD score ON likes TYPE number INLINE;
INFO FOR TABLE likes;

-- ALTER can set and drop the clause.
DEFINE FIELD tag ON likes TYPE option<string>;
ALTER FIELD tag ON likes INLINE;
ALTER FIELD score ON likes DROP INLINE;
INFO FOR TABLE likes;

-- Rejections: INLINE needs an existing relation's stored edges.
DEFINE FIELD score ON person TYPE number INLINE;
DEFINE FIELD score ON missing TYPE number INLINE;
DEFINE FIELD in ON likes TYPE record<person> INLINE;
DEFINE FIELD meta.nested ON likes TYPE number INLINE;
DEFINE FIELD calc ON likes TYPE number COMPUTED score * 2 INLINE;
DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
DEFINE FIELD score ON follows TYPE number INLINE;

-- A relation with INLINE fields cannot be demoted to another type.
ALTER TABLE likes TYPE NORMAL;
ALTER FIELD tag ON likes DROP INLINE;
ALTER TABLE likes TYPE NORMAL;
INFO FOR TABLE likes;
