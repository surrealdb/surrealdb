/**
[env]
namespace = "metadata_ns"
database = "metadata_db"

[test]
issue = 2194
reason = "SQL session metadata exposes the selected database and a numeric version consistent with the PostgreSQL listener."

[[test.results]]
value = "[{ db: 'metadata_db' }]"
[[test.results]]
value = "[{ v: '160000' }]"
*/
SELECT current_database() AS db;
SELECT current_setting('server_version_num') AS v;
