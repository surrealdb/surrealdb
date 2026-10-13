/**
[env]
imports = ["upgrade/define/3.00.00_value_depth_key_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# Only a binary that does not bound a stored value's nesting depth can write the
# state, and only one that does can show the bound holding, so the hop is pinned
# from both ends. The pair together also keeps the ordinary language-test runner
# away: it matches both fields against its own version, and replaying the import
# against a build that enforces the bound would fail at the write.
importing-version = "<3.4.0-0"
version = ">=3.4.0-a"
# The depth bound guards the write path only. Reading a record id nested past it
# recurses once per level through the derived `DeserializeRevisioned` impls for
# `Value` and `Array`, which carry no depth limit, so a row an older release
# wrote can exhaust the stack instead of being rejected.
#
# It aborts on rocksdb because that backend can dispatch a scan visitor, record
# decode included, onto the shared KVS pool, whose threads take the default
# stack rather than the larger one a runtime worker gets. Which of the two a
# given read uses is decided by whether an inline permit is free, so this is a
# property of the dispatch and the stack sizes, not of the backend: surrealkv
# passes because it keeps the scan visitor off that pool.
wip = true
reason = """
A compound record id nests through arrays and objects just as a field value
does, but it is stored in the key rather than in the value. That makes it a
distinct read path: the id is reconstructed from the storage key by whatever
walks the table, and it is grafted onto the record before any part of the value
is looked at. A depth bound applied only to what was serialised into the value
would leave this path unguarded, and a scan touching the row would walk the deep
key regardless of what the query projects.

value_depth.surql cannot show this. There the deep nesting is inside the stored
value, so a reader that treats a record id as a leaf still passes it. Keeping
the two in separate files is what makes a failure name the path that broke.

The row can only be created by a binary that does not bound the depth, so the
seed axis is what carries it: every released version writes the row and hands it
straight to the current checkout.

The body never projects the over-deep id. The bound is applied while a result is
converted for the caller, and a conversion that fails ends the whole batch
rather than the one statement — so that the id nests past the bound is
established by the write refusal, which walks the id as part of the record and
reports per statement."""

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "['deep']"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "0"

[[test.results]]
value = "[up_vdepth_key:[1]]"

*/
-- Both rows the old binary left behind are still in the table, the deep-keyed
-- one included: a scan over the key range counts it like any other.
SELECT count() FROM up_vdepth_key GROUP ALL;
-- The deep-keyed row's own fields read back, which means its record was
-- assembled: the id is reconstructed from the storage key and grafted onto the
-- record before any field is looked at, and that decode applies no depth bound
-- of its own. Only the projection leaves the id out.
SELECT VALUE marker FROM up_vdepth_key WHERE marker = 'deep';
-- Rewriting that row is refused. The id is part of the record walked on the way
-- back to storage, so a key nested past the bound is rejected exactly as a
-- field value would be — and that is what establishes the row above as a
-- genuine violation rather than an ordinary record.
UPDATE up_vdepth_key SET w = 1 WHERE marker = 'deep' RETURN NONE;
-- The refusal fails closed: the rejected write left nothing behind, so no row
-- carries the field it would have set.
RETURN array::len((SELECT VALUE marker FROM up_vdepth_key WHERE w = 1));
-- Liveness, and the reason it comes last: the server is still serving after the
-- refusal, and a compound id the same old binary wrote within the bound is
-- still returned whole. The bound rejects the one record that crosses it, not
-- the table it shares with the others.
SELECT VALUE id FROM up_vdepth_key WHERE marker = 'shallow';
