/**
[test]
reason = "Chain setup for field_reference.surql: defines the REFERENCE field with its ON DELETE behaviour and the COMPUTED back-traversal field on the first walked version, then writes the author and the one book every later version links against."
run = false
*/
DEFINE TABLE chain_field_reference_author SCHEMALESS;
DEFINE TABLE chain_field_reference_book SCHEMALESS;
DEFINE FIELD author ON chain_field_reference_book TYPE record<chain_field_reference_author> REFERENCE ON DELETE UNSET;
DEFINE FIELD books ON chain_field_reference_author COMPUTED <~chain_field_reference_book;
CREATE chain_field_reference_author:seed SET name = 'Alice', wrote_at = $version;
-- Record id 0 keeps the first version's book ahead of every `mutate` book,
-- which numbers its own by hop from 1 upwards.
CREATE chain_field_reference_book:0 SET title = 'seed', author = chain_field_reference_author:seed, wrote_at = $version;
