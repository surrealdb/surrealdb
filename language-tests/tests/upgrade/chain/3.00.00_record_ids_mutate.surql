/**
[test]
reason = "Chain mutation for record_ids.surql: every walked version adds one record per id encoding, keyed off $hop so no two versions write the same id, into the table an earlier version created."
run = false
*/
CREATE type::record('chain_record_ids', $hop) SET shape = 'int', wrote_at = $version;
CREATE type::record('chain_record_ids', 'hop_' + <string> $hop) SET shape = 'ident', wrote_at = $version;
CREATE type::record('chain_record_ids', 'hop ' + <string> $hop) SET shape = 'text', wrote_at = $version;
