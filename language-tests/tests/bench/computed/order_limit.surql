/**
[bench]
reason = "ORDER BY … LIMIT over a read-time COMPUTED sort key, against the same values held in an ordinary stored field — the TopK threshold probe reads the sort key from revision-encoded bytes and is disarmed for a computed field, so every row pays a full decode and a derivation instead of being rejected against the heap's worst entry"
run = true
rebuild = false
datasets = { stored = "bench/util/records-10000-bucket-stored.surql", computed = "bench/util/records-10000-computed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, bucket FROM record ORDER BY bucket DESC LIMIT 20;
