/**
[env]
imports = ["upgrade/define/index_unique_import.surql"]

[test]
upgrade = true
reason = "Unique index survives upgrade, still enforces uniqueness, and lookup by indexed column returns the expected row"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { idx_uniq_email: 'DEFINE INDEX idx_uniq_email ON idx_uniq FIELDS email UNIQUE' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ email: 'alice@example.com', id: idx_uniq:1 }]"

[[test.results]]
error = true

*/
INFO FOR TABLE idx_uniq;
SELECT * FROM idx_uniq WITH INDEX idx_uniq_email WHERE email = 'alice@example.com';
-- Inserting a duplicate must still fail post-upgrade.
CREATE idx_uniq:3 SET email = 'alice@example.com';
