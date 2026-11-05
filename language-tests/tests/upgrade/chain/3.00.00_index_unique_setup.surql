/**
[test]
reason = "Chain setup for index_unique.surql: creates the UNIQUE index and the one row with a stable record id on the first walked version. Unique indexes exist in every 3.x release, so this runs at the head of the chain."
run = false
*/
DEFINE INDEX chain_uniq_email ON TABLE chain_uniq COLUMNS email UNIQUE;
CREATE chain_uniq:first SET email = 'first@example.com', hop = 0;
