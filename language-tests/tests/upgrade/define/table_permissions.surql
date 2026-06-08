/**
[env]
imports = ["upgrade/define/table_permissions_import.surql"]

[test]
upgrade = true
reason = "Per-action PERMISSIONS expressions on a table (mixed FULL/NONE/WHERE clauses) survive upgrade — the expression text is preserved verbatim and remains effective post-upgrade for root queries"

[[test.results]]
match = '''
    string::contains($result.tables.doc, "DEFINE TABLE doc")
    && string::contains($result.tables.doc, "SCHEMALESS")
    && string::contains($result.tables.doc, "FOR select WHERE owner = $auth.id OR $access = 'public'")
    && string::contains($result.tables.doc, "FOR create, update WHERE owner = $auth.id")
    && string::contains($result.tables.doc, "FOR delete NONE")
'''

[[test.results]]
value = "[{ id: doc:1, owner: 'alice', title: 'first' }, { id: doc:2, owner: 'bob', title: 'second' }]"

*/
INFO FOR DB;
-- Root always bypasses table-level permissions, so the stored rows are still
-- visible. The relevant assertion is that the PERMISSIONS expression text
-- survived intact (above).
SELECT * FROM doc ORDER BY id;
