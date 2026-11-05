/**
[test]
reason = "Setup for graph_where_on_a_hidden_edge_key and graph_where_on_an_edge_key_an_owner_reads: an edge table whose `out` field a record user may not select. An adjacency entry's key holds the raw `out`, so a record user's hop `WHERE` over it must be judged where `out` reads NONE."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE knows SCHEMALESS TYPE RELATION PERMISSIONS FULL;
DEFINE FIELD OVERWRITE out ON knows TYPE record<person> PERMISSIONS FOR select WHERE false;

CREATE person:me, person:friend, person:secret;
RELATE person:me->knows:to_friend->person:friend;
RELATE person:me->knows:to_secret->person:secret;
