/**
[test]
run = false
reason = "Setup only: a source table of three rows and two empty destinations, read by qualified_wildcard_insert, qualified_wildcard_insert_other_table and qualified_wildcard_insert_with_item"
*/

DEFINE TABLE qw_src SCHEMALESS;
DEFINE TABLE qw_dst SCHEMALESS;
DEFINE TABLE qw_dst2 SCHEMALESS;

CREATE qw_src:1 SET k = 1, name = 'ada' RETURN NONE;
CREATE qw_src:2 SET k = 2, name = 'bob' RETURN NONE;
CREATE qw_src:3 SET k = 3, name = 'cy' RETURN NONE;
