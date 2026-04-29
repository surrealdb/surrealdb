/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "SCHEMAFULL TYPE object rejects undeclared nested fields (not silent data loss)"
issue = 7240

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'metadata.category', but no such field exists for table 'document'"

[[test.results]]
value = "[]"
*/

-- SCHEMAFULL + TYPE object (no FLEXIBLE) should reject undeclared nested fields
DEFINE TABLE document SCHEMAFULL;
DEFINE FIELD metadata ON document TYPE object;

-- This insert should error because metadata.category and metadata.source are not defined
INSERT INTO document {
  id: document:doc1,
  metadata: { category: "basic", source: "helpdesk" }
};

-- No data should exist since the insert was rejected
SELECT * FROM document:doc1;
