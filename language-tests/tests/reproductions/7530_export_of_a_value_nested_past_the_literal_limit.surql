/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A record may hold a value nested deeper than a query literal may be written, and its export reads back in full (cli_integration export_of_a_deeply_nested_value_imports_back); an import with a statement that cannot apply answers 422"
issue = 7530

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:deep SET content = {leaf: true} RETURN NONE;
FOR $i IN 0..110 { UPDATE t:deep SET content = {children: [content]} RETURN NONE; };
SELECT VALUE string::len(<string> content) > 1000 FROM ONLY t:deep;
