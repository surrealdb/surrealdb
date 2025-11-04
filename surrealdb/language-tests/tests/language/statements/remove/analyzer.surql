/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The analyzer 'example_blank' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The analyzer 'example_blank' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE ANALYZER example_blank TOKENIZERS blank;
DEFINE ANALYZER IF NOT EXISTS example_blank TOKENIZERS blank;
DEFINE ANALYZER OVERWRITE example_blank TOKENIZERS blank;
DEFINE ANALYZER example_blank TOKENIZERS blank;
REMOVE ANALYZER IF EXISTS example_blank;
REMOVE ANALYZER example_blank;
REMOVE ANALYZER IF EXISTS example_blank;
