/**
[env]
imports = ["upgrade/define/3.00.00_catalog_object_versions_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# Only the current binary versions catalog objects, and older readers do not
# parse `ALTER PARAM`.
version = ">=3.4.0-a"
reason = "Objects an older binary wrote carry no catalog object version; the current binary reads them as unversioned, serves them, and redefines, alters and removes them through the path that mints their first version, after which every read sees the new definition."

[[test.results]]
value = "1"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
match = '''type::is_int($result) && $result >= 10'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "['A', 'B']"

[[test.results]]
match = '''type::is_int($result) && $result >= 100'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''$result.params == {} && $result.analyzers == {} && string::contains($result.sequences.versioned, "START 100")'''

*/
-- Each object as the older binary left it.
$versioned;
search::analyze('versioned', 'A B');
sequence::nextval('versioned');
-- Each object changed by the current binary.
ALTER PARAM $versioned VALUE 2;
DEFINE ANALYZER OVERWRITE versioned TOKENIZERS blank;
DEFINE SEQUENCE OVERWRITE versioned START 100;
-- Every read sees the changed definitions.
$versioned;
search::analyze('versioned', 'A B');
sequence::nextval('versioned');
-- Removed, they are gone; the sequence, left alone, stays.
REMOVE PARAM $versioned;
REMOVE ANALYZER versioned;
INFO FOR DB;
