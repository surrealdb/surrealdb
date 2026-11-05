/**
[env]
namespace = true
database = true

[test]
reason = "A keyword-named index column must be escaped the same way by both INFO forms. STRUCTURE rendered `cols` through EscapeKwFreeIdent, which leaves a reserved word bare, so one definition had two spellings: `value` here and `` `value` `` in plain INFO."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE INDEX i ON `select` FIELDS `value`'"

[[test.results]]
value = "['`value`']"
*/

-- `select` and `value` are both reserved words, so both need escaping to read
-- back. No other test in the tree uses a keyword-named index column.
DEFINE TABLE `select`;
DEFINE INDEX i ON `select` FIELDS `value`;

-- Plain INFO renders the whole definition, and always escaped it.
RETURN (INFO FOR TABLE `select`).indexes.i;

-- STRUCTURE exposes the columns separately; it must not escape more weakly.
RETURN (INFO FOR TABLE `select` STRUCTURE).indexes[0].cols;
