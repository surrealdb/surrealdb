/**
[test]
reason = "Chain setup for field_computed.surql: defines the SCHEMAFULL table carrying a computed VALUE field alongside a READONLY DEFAULT field, and seeds the one row every later version rewrites. Field clauses exist in every 3.x release, so this runs at the head of the chain."
run = false
*/
DEFINE TABLE chain_field_computed SCHEMAFULL;
DEFINE FIELD name ON chain_field_computed TYPE { first: string, last: string };
DEFINE FIELD full_name ON chain_field_computed TYPE string VALUE string::concat(name.first, ' ', name.last);
DEFINE FIELD created_at ON chain_field_computed TYPE datetime READONLY DEFAULT d'2025-01-01T00:00:00Z';
DEFINE FIELD revision ON chain_field_computed TYPE int DEFAULT 0;
DEFINE FIELD hop ON chain_field_computed TYPE int;
DEFINE FIELD wrote_at ON chain_field_computed TYPE string;
-- The row the body rewrites at every hop. `hop = 0` keeps it out of the
-- per-version rows the body counts.
CREATE chain_field_computed:seed SET name = { first: 'Alice', last: 'Rev0' }, hop = 0, wrote_at = $version;
