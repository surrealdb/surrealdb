/**
[env]
imports = ["upgrade/define/table_schemafull_import.surql"]

[test]
upgrade = true
reason = "SCHEMAFULL table with field-level TYPE, ASSERT, and DEFAULT constraints survives upgrade — pre-populated rows are readable AND the constraints are still enforced on post-upgrade INSERTs"

[[test.results]]
match = '''
    string::contains($result.tables.account, "SCHEMAFULL")
'''

[[test.results]]
match = '''
    string::contains($result.fields.name, "TYPE string ASSERT $value != NONE")
    && string::contains($result.fields.age, "TYPE int ASSERT")
    && string::contains($result.fields.email, "string::is_email")
    && string::contains($result.fields.tags, "TYPE array<string>")
    && string::contains($result.fields.tags, "DEFAULT []")
'''

[[test.results]]
error = true

[[test.results]]
error = true

*/
INFO FOR DB;
INFO FOR TABLE account;
-- Constraints must still fire post-upgrade.
CREATE account:3 SET name = 'Carol', age = -5, email = 'carol@example.com';
CREATE account:4 SET name = 'Dave', age = 40, email = 'not-an-email';
