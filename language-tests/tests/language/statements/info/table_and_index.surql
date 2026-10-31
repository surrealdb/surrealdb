/**
[test]
reason = "INFO FOR TABLE returns the table's events/fields/indexes/lives/tables map. INFO FOR INDEX returns the named index's build status."

[env]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { info_idx: 'DEFINE INDEX info_idx ON info_tbl FIELDS v' }, lives: {  }, tables: {  } }"

# `building.queued` and `building.active` follow the background compaction drain,
# which a server runs and the in-process harness does not, so they are not pinned.
[[test.results]]
match = "$result.building.{ armed_shards, compacting, initial, pending, status, updated } == { armed_shards: 0, compacting: false, initial: 0, pending: 0, status: 'ready', updated: 0 }"

*/

{
    DEFINE TABLE info_tbl;
    DEFINE INDEX info_idx ON info_tbl FIELDS v;
    CREATE info_tbl:1 SET v = 1;
    CREATE info_tbl:2 SET v = 2;
    RETURN NONE;
};

INFO FOR TABLE info_tbl;
INFO FOR INDEX info_idx ON info_tbl;
