/**
[test]
reason = "Chain setup for record_ids.surql: writes one record for each id encoding the chain follows — an integer key, a bare identifier key, and a quoted string key containing whitespace. All three encodings exist in every 3.x release, so this runs at the head of the chain. The keys chosen here are outside the range mutate generates, so hop 1 does not collide with its own setup."
run = false
*/
CREATE chain_record_ids:0 SET shape = 'int', wrote_at = $version;
CREATE chain_record_ids:seed SET shape = 'ident', wrote_at = $version;
CREATE chain_record_ids:`seed record` SET shape = 'text', wrote_at = $version;
