/**
[test]
reason = "Chain mutation for index_standard.surql: every walked version writes its own row through the index the first version created, so each release maintains a structure an older release built."
run = false
*/
CREATE chain_std SET org = 'surrealdb', wrote_at = $version;
