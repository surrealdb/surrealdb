/**
[env]
namespace = true
database = true

[test]
reason = "INSERT IGNORE of an existing record leaves one row, and an indexed SELECT returns it once."
issue = 6780

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: m:a, storeId: 253 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: m:a, storeId: 253 }]"
*/
DEFINE INDEX idx_store ON TABLE m FIELDS storeId;
INSERT IGNORE INTO m { id: 'a', storeId: 253 };
INSERT IGNORE INTO m { id: 'a', storeId: 253 };
INSERT IGNORE INTO m [{ id: 'a', storeId: 254 }, { id: 'a', storeId: 253 }];
SELECT * FROM m WHERE storeId = 253;
