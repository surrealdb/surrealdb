/**
[test]
reason = "Chain mutation for table_schemafull.surql: every walked version writes one conforming row through the schema the first version defined. The id is generated and the values derive from $hop, so no two versions collide. `tags` is left unset so each release exercises the DEFAULT clause it inherited."
run = false
*/
CREATE chain_table_schemafull SET
	name = string::concat('user-', <string> $hop),
	age = 20 + $hop,
	email = string::concat('user', <string> $hop, '@example.com'),
	hop = $hop,
	wrote_at = $version;
