/**
[env]
namespace = true
database = true

[test]
reason = "A CREATE whose content comes from parameters persists after the schema was defined in earlier queries, and a SCHEMAFULL rejection of the payload is reported as that statement's error instead of being silently dropped. The SDK-level check lives in surrealdb/tests/embedded_bound_params_persist.rs; the report's `.await?` does not surface per-statement errors, and its payload object was undeclared on a non-FLEXIBLE field."
issue = 6735

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ entity_id: 'test123', entity_type: 'test', payload: { key: 'value' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'payload.key', but no such field exists for table 'strict'"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE TABLE sync_queue SCHEMAFULL;
DEFINE FIELD entity_type ON TABLE sync_queue TYPE string;
DEFINE FIELD entity_id ON TABLE sync_queue TYPE string;
DEFINE FIELD payload ON TABLE sync_queue TYPE object FLEXIBLE;
DEFINE FIELD created_at ON TABLE sync_queue TYPE datetime;
DEFINE INDEX idx_entity ON TABLE sync_queue COLUMNS entity_type, entity_id;
DEFINE TABLE other_table SCHEMALESS;
LET $entity_type = 'test';
LET $entity_id = 'test123';
LET $payload = { key: 'value' };
CREATE sync_queue CONTENT { entity_type: $entity_type, entity_id: $entity_id, payload: $payload, created_at: time::now() } RETURN NONE;
SELECT entity_type, entity_id, payload FROM sync_queue WHERE entity_type = 'test' AND entity_id = 'test123';
DEFINE TABLE strict SCHEMAFULL;
DEFINE FIELD payload ON strict TYPE object;
CREATE strict CONTENT { payload: $payload };
SELECT count() FROM strict GROUP ALL;
