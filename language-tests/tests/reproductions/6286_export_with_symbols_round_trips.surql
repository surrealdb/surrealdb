/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Text outside ASCII survives a dump. An export written to stdout carries no log lines (cli_integration export_to_stdout_is_the_dump_alone), and an import accepts a UTF-8 byte order mark and names UTF-16 input"
issue = 6286

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: s:1, textVal: '✓ 🧘‍♀️ ⟨x⟩' }]"

[[test.results]]
value = "'✓ 🧘‍♀️ ⟨x⟩'"
*/

DEFINE TABLE s SCHEMALESS;
CREATE s:1 SET textVal = '✓ 🧘‍♀️ ⟨x⟩';
SELECT VALUE textVal FROM ONLY s:1;
