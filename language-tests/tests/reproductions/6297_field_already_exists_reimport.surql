/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Array fields auto-generate sub-field definitions; re-defining with OVERWRITE (as export now produces) must not error"
issue = 6297

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
value = """{ events: {  }, fields: { embedding: 'DEFINE FIELD embedding ON chunk TYPE none | array<float> PERMISSIONS FULL', "embedding.*": 'DEFINE FIELD embedding.* ON chunk TYPE float PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = """{ events: {  }, fields: { extracted: 'DEFINE FIELD extracted ON document TYPE object PERMISSIONS FULL', "extracted.authors": 'DEFINE FIELD extracted.authors ON document TYPE array<string> DEFAULT [] PERMISSIONS FULL', "extracted.authors.*": 'DEFINE FIELD extracted.authors.* ON document TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"""
*/

-- Define a table with an array field (auto-generates embedding[*])
DEFINE TABLE OVERWRITE chunk TYPE NORMAL SCHEMALESS PERMISSIONS NONE;
DEFINE FIELD OVERWRITE embedding ON chunk TYPE option<array<float>> PERMISSIONS FULL;

-- Simulate what the fixed export produces: re-define the auto-generated sub-field with OVERWRITE
DEFINE FIELD OVERWRITE embedding[*] ON chunk TYPE float PERMISSIONS FULL;

-- Also test nested object array fields
DEFINE TABLE OVERWRITE document TYPE NORMAL SCHEMALESS PERMISSIONS NONE;
DEFINE FIELD OVERWRITE extracted ON document TYPE object PERMISSIONS FULL;
DEFINE FIELD OVERWRITE extracted.authors ON document TYPE array<string> DEFAULT [] PERMISSIONS FULL;

-- Re-define the auto-generated sub-field with OVERWRITE
DEFINE FIELD OVERWRITE extracted.authors[*] ON document TYPE string PERMISSIONS FULL;

-- Verify tables are intact
INFO FOR TABLE chunk;
INFO FOR TABLE document;
