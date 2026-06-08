/**
[env]
imports = ["upgrade/define/field_default_always_import.surql"]

[test]
upgrade = true
reason = "DefineDefault::Always survives upgrade — the DEFAULT ALWAYS clause round-trips in the catalog AND still triggers on post-upgrade writes (covers the DefineDefault::Always variant distinct from DefineDefault::Set)"

[[test.results]]
match = '''string::contains($result.fields.label, "DEFAULT ALWAYS 'auto'")'''

[[test.results]]
value = "[{ id: doc:1, label: 'pre-upgrade' }]"

[[test.results]]
value = "[{ id: doc:1, label: 'auto' }]"

*/
INFO FOR TABLE doc;
SELECT * FROM doc:1;
-- Setting the field to NONE should trigger DEFAULT ALWAYS, restoring 'auto'.
UPSERT doc:1 SET label = NONE;
