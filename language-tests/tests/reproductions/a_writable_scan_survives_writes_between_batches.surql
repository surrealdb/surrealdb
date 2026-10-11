/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "A statement that writes while its own table scan is still open must read the rest of the scan intact. Every row of `t` is a pending write of the open transaction, so each scan batch ends with the iterator parked on an entry held in the transaction's write batch; the writes made between batches (the UPDATE itself, an event body, or a subquery's CREATE) grow and reallocate that buffer. The RocksDB iterator caches the value of its current entry as a slice into the buffer, so without re-reading it the next batch copied freed memory (heap-use-after-free under ASAN)."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1000 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 2000 }]"

[[test.results]]
value = "[{ count: 2000 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3000"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 3000 }]"
*/

-- Rows are created in the same transaction as the statement that scans them,
-- so the scan reads them from the transaction's write batch rather than from
-- committed storage.
BEGIN;
FOR $i IN 0..1000 { CREATE type::record('t', $i) SET v = string::repeat('a', 64); };
UPDATE t SET v = string::repeat('b', 1000) RETURN NONE;
COMMIT;
SELECT count() FROM t WHERE v = string::repeat('b', 1000) GROUP ALL;

-- Every row's event body writes, between scan batches, a row of its own.
DEFINE EVENT touched ON t WHEN $event = 'UPDATE' THEN {
	CREATE type::record('log', $after.id.id()) SET v = string::repeat('e', 1000);
};
BEGIN;
FOR $i IN 1000..2000 { CREATE type::record('t', $i) SET v = string::repeat('a', 64); };
UPDATE t SET v = string::repeat('c', 1000) RETURN NONE;
COMMIT;
SELECT count() FROM t WHERE v = string::repeat('c', 1000) GROUP ALL;
SELECT count() FROM log GROUP ALL;

-- A subquery's CREATE writes while the outer SELECT is still scanning.
REMOVE EVENT touched ON t;
BEGIN;
FOR $i IN 2000..3000 { CREATE type::record('t', $i) SET v = string::repeat('a', 64); };
RETURN array::len(SELECT VALUE (CREATE ONLY type::record('sub', id.id()) SET v = string::repeat('s', 1000) RETURN VALUE 1) FROM t);
COMMIT;
SELECT count() FROM sub GROUP ALL;
