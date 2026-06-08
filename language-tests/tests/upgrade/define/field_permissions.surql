/**
[env]
imports = ["upgrade/define/field_permissions_import.surql"]

[test]
upgrade = true
reason = "Per-field PERMISSIONS (FieldDefinition.permissions slot) round-trip through upgrade — both `PERMISSIONS FULL` and the per-action `FOR select WHERE ... FOR update NONE` shape are preserved verbatim"

[[test.results]]
match = '''
    string::contains($result.fields.email, "PERMISSIONS FULL")
    && string::contains($result.fields.ssn, "FOR select WHERE $auth.admin = true")
    && string::contains($result.fields.ssn, "FOR update NONE")
'''

*/
INFO FOR TABLE acct;
