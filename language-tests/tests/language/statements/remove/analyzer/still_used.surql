/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: Cannot delete analyzer `example_blank` which is used by index(es) `example.example_index`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/


DEFINE ANALYZER example_blank TOKENIZERS blank;
DEFINE INDEX example_index ON example FIELDS name FULLTEXT ANALYZER example_blank;
REMOVE ANALYZER example_blank;

REMOVE INDEX example_index ON example;
REMOVE ANALYZER example_blank;
