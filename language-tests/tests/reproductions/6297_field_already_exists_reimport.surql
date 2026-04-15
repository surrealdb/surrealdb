/**
[env]
namespace = true
database = true

[test]
reason = "Re-import after export should not fail with 'field already exists' for array sub-fields"
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
value = """{ embedding: 'DEFINE FIELD embedding ON chunk TYPE none | array<float> PERMISSIONS FULL', "embedding.*": 'DEFINE FIELD embedding.* ON chunk TYPE float PERMISSIONS FULL' }"""

[[test.results]]
value = """{ "extracted.authors": "DEFINE FIELD extracted.authors ON document TYPE array<string> DEFAULT [] COMMENT 'Authors of the document' PERMISSIONS FULL", "extracted.authors.*": 'DEFINE FIELD extracted.authors.* ON document TYPE string PERMISSIONS FULL' }"""

*/
OPTION IMPORT;
DEFINE TABLE OVERWRITE chunk TYPE NORMAL SCHEMALESS PERMISSIONS NONE;
DEFINE FIELD embedding ON chunk TYPE option<array<float>> PERMISSIONS FULL;
DEFINE FIELD embedding[*] ON chunk TYPE float PERMISSIONS FULL;
DEFINE TABLE OVERWRITE document TYPE NORMAL SCHEMALESS PERMISSIONS NONE;
DEFINE FIELD extracted.authors ON document TYPE array<string> DEFAULT [] COMMENT 'Authors of the document' PERMISSIONS FULL;
DEFINE FIELD extracted.authors[*] ON document TYPE string PERMISSIONS FULL;

(INFO FOR TB chunk).fields;
(INFO FOR TB document).fields;
