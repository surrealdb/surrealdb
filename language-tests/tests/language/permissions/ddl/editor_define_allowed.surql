/**
[env]
auth = { level = "editor" }

[test]
reason = "Editor can DEFINE/REMOVE/ALTER: Table, Field, Index, Function, Analyzer, Param, Event"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
-- Define
DEFINE TABLE editor_tbl;
DEFINE FIELD name ON editor_tbl TYPE string;
DEFINE INDEX idx ON editor_tbl FIELDS name;
DEFINE EVENT ev ON editor_tbl WHEN true THEN {};
DEFINE PARAM $editor_p VALUE 42;
DEFINE FUNCTION fn::editor_fn() { RETURN 1; };
DEFINE ANALYZER editor_az TOKENIZERS blank;
-- Alter
ALTER TABLE editor_tbl COMMENT 'edited';
-- Remove (reverse order)
REMOVE ANALYZER editor_az;
REMOVE FUNCTION fn::editor_fn;
REMOVE PARAM $editor_p;
REMOVE EVENT ev ON editor_tbl;
REMOVE INDEX idx ON editor_tbl;
REMOVE FIELD name ON editor_tbl;
REMOVE TABLE editor_tbl;
