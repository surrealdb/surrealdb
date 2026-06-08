/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "REMOVE ANALYZER should reject removal while a full-text index still references the analyzer; removing the index first should permit the removal."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The analyzer 'in_use_az' is in use by index 'in_use_ix' on table 'doc' and cannot be removed"

[[test.results]]
error = "The analyzer 'in_use_az' is in use by index 'in_use_ix' on table 'doc' and cannot be removed"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE ANALYZER in_use_az TOKENIZERS blank;
DEFINE INDEX in_use_ix ON doc FIELDS text FULLTEXT ANALYZER in_use_az BM25(1.2, 0.75);
REMOVE ANALYZER in_use_az;
REMOVE ANALYZER IF EXISTS in_use_az;
REMOVE INDEX in_use_ix ON doc;
REMOVE ANALYZER in_use_az;
