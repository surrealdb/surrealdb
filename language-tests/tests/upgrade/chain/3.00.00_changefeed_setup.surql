/**
[test]
reason = "Chain setup for changefeed.surql: defines the change feed on the first walked version and writes the one entry every later hop still has to see. The retention window has to outlast the whole walk, because the body reads SHOW CHANGES SINCE 0 at every hop and an expired window would silently drop the entries the earliest binaries wrote."
run = false
*/
DEFINE TABLE chain_cf_src SCHEMALESS CHANGEFEED 1h;
CREATE chain_cf_src:seed SET wrote_at = $version;
