/**
[test]

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
value = """
{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { \"\": 'DEFINE TABLE `` TYPE ANY SCHEMALESS PERMISSIONS NONE', \"\u0000\": 'DEFINE TABLE `\u0000` TYPE ANY SCHEMALESS PERMISSIONS NONE', \"\t\": 'DEFINE TABLE `\t` TYPE ANY SCHEMALESS PERMISSIONS NONE', \"
\": 'DEFINE TABLE `
` TYPE ANY SCHEMALESS PERMISSIONS NONE', \"\f\": 'DEFINE TABLE `\f` TYPE ANY SCHEMALESS PERMISSIONS NONE', \"\r\": 'DEFINE TABLE `\r` TYPE ANY SCHEMALESS PERMISSIONS NONE', \"\\\"\": 'DEFINE TABLE `\"` TYPE ANY SCHEMALESS PERMISSIONS NONE', \"'\": \"DEFINE TABLE `'` TYPE ANY SCHEMALESS PERMISSIONS NONE\", S: 'DEFINE TABLE S TYPE ANY SCHEMALESS PERMISSIONS NONE', U: 'DEFINE TABLE U TYPE ANY SCHEMALESS PERMISSIONS NONE', \"\\\\\": 'DEFINE TABLE `\\\\\\\\` TYPE ANY SCHEMALESS PERMISSIONS NONE', \"`\": 'DEFINE TABLE `\\\\`` TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"""

*/

DEFINE TABLE ``;
DEFINE TABLE `\0`;
DEFINE TABLE `\n`;
DEFINE TABLE `\r`;
DEFINE TABLE `\t`;
DEFINE TABLE `\f`;
DEFINE TABLE `\\`;
DEFINE TABLE `\"`;
DEFINE TABLE `\'`;
DEFINE TABLE `\``;
DEFINE TABLE `\u0053`;
DEFINE TABLE `\u{55}`;
INFO FOR DB;
