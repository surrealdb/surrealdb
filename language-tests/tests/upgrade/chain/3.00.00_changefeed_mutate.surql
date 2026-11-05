/**
[test]
reason = "Chain mutation for changefeed.surql: every walked version appends one entry to the change feed an older version started. The write is an UPSERT of a fixed record id so it replays at every hop without colliding, while still emitting a distinct change entry stamped with the binary that wrote it."
run = false
*/
UPSERT chain_cf_src:tracker SET hop = $hop, wrote_at = $version;
