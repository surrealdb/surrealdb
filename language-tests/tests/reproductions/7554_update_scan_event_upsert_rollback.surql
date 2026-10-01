/**
[env]
namespace = true
database = true
backend = ["mem", "rocksdb"]

[test]
reason = "An UPSERT inside an event rolls back its savepoint when the record already exists, while the outer UPDATE's scan cursor is open. On RocksDB that rolled back under a live cursor and panicked the server; the scan must carry on and update every row."
issue = 7554

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
value = "[]"

[[test.results]]
value = "[chat:1, chat:2, chat:3, chat:4, chat:5, chat:6]"

[[test.results]]
value = "6"

*/

DEFINE TABLE chat SCHEMALESS;
DEFINE TABLE contribution SCHEMALESS;
DEFINE TABLE total SCHEMALESS;
FOR $n IN 1..=6 {
	CREATE type::record('chat', $n) CONTENT { title: <string>$n };
};
DEFINE FUNCTION fn::refresh($source: record) {
	LET $key = type::record('contribution', crypto::sha256(<string>$source));
	LET $previous = SELECT * FROM ONLY $key;
	UPSERT $key SET source = $source, amount = 0dec;
};
DEFINE EVENT totals ON contribution WHEN $event IN ['CREATE', 'UPDATE'] THEN {
	UPSERT total:chats SET count = (count ?? 0) + 1;
};
DEFINE EVENT refresh ON chat WHEN $event = 'UPDATE' THEN { fn::refresh($after.id); };
UPDATE chat SET scope = 'all' WHERE scope = NONE RETURN NONE;
SELECT VALUE id FROM chat WHERE scope = 'all' ORDER BY id;
SELECT VALUE count FROM ONLY total:chats;
