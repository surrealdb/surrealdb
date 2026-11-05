/**
[env]
imports = ["upgrade/define/3.00.00_index_btree_decimal_spelling_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# The plans asserted are those of an index stamped below the format that spells
# decimals without trailing zeros, which only a released binary writes: the
# current checkout stamps the index it defines at that format, and reads an
# open range over it from the index. So the source must predate the format, and
# this gate also keeps the ordinary runner away, since it matches the gate
# against its own version.
importing-version = "<3.4.0-0"
version = ">=3.4.0-a"
reason = """
The plans the reads in 3.00.00_index_btree_decimal_spelling.surql are answered
by on an index the upgrade target did not rebuild, so stamped below the format
that spells decimals without trailing zeros. An equality and a `UNIQUE` lookup
are the index scan's own access, and a closed range is read from the index with
each record it fetches checked against the condition, so the rows returned are
what the old index holds and a range bound on `2.5dec` never admits `2.50dec`.
Such an index answers no numeric range exactly, so an open range is read from
the table. An index the new binary cannot read would fall back to a table scan
for every read and hide the missing rows the sibling asserts.

`EXPLAIN` output is not stable across the 3.x line, so the upgrade target is the
current checkout; the seed axis sends every released version straight to it."""

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: dec_idx, index: dec_idx_c, access: = 2.5dec, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: dec_idx, direction: Forward, predicate: c >= 2.5dec, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: dec_uniq, index: dec_uniq_c, access: = 2.5dec, direction: Forward]
''''

[[test.results]]
value = "[dec_idx:3]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [table: dec_idx, index: dec_idx_c, access: >2.5dec <3dec, direction: Forward]
''''

*/
EXPLAIN SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c = 2.5dec;
EXPLAIN SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c >= 2.5dec;
EXPLAIN SELECT VALUE id FROM dec_uniq WITH INDEX dec_uniq_c WHERE c = 2.5dec;
SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c > 2.5dec AND c < 3dec ORDER BY id;
EXPLAIN SELECT VALUE id FROM dec_idx WITH INDEX dec_idx_c WHERE c > 2.5dec AND c < 3dec;
