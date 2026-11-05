/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Carries one change feed through every version in the chain: the feed is defined
once, and from there every release in turn appends to the Cf key range its
predecessors wrote and reads the whole range back from versionstamp 0.

Versionstamp ordering across binaries is what a pair cannot observe. A pair sees
a single handover, so a release that rewinds or restarts the versionstamp oracle
still orders its own entries correctly and passes. Here every earlier binary's
entries stay in the same feed, and the ordering assertion has to hold across
every handover at once — a rewind puts the new binary's append in front of
entries an older one already wrote."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_changefeed_setup.surql"
mutate = "upgrade/chain/3.00.00_changefeed_mutate.surql"

[[test.results]]
match = '''string::contains($result.tables.chain_cf_src, "CHANGEFEED 1h")'''

[[test.results]]
match = '''
    type::is_array($result)
    && array::len($result) == $hop + 2
    && $result.versionstamp == array::sort($result.versionstamp)
    && array::len(array::distinct($result.versionstamp)) == array::len($result)
    && count(array::flatten($result.changes)[WHERE $this.update.id == chain_cf_src:seed]) == 1
    && count(array::flatten($result.changes)[WHERE $this.update.id == chain_cf_src:tracker]) == $hop
    && array::last($result).changes[0].update.id == chain_cf_src:tracker
    && array::last($result).changes[0].update.hop == $hop
    && array::last($result).changes[0].update.wrote_at == $version
'''

*/
INFO FOR DB;
-- The whole feed: the entry the first walked version wrote, plus one append per
-- hop since. The assertions on it, in order.
--
-- The entry total is exact: the DEFINE TABLE, the seed CREATE, and one append
-- per hop since. A floor here would still pass a release that stopped recording
-- changes altogether, which is the regression this test exists to catch.
--
-- array::len, not count(): count() tallies only truthy elements, so a
-- versionstamp of 0 would silently drop out of the distinctness check.
--
-- Versionstamps strictly increase over the whole feed. This is what the walk
-- exists to check: every entry but the last was written by a different binary,
-- so it fails the moment a release hands out a versionstamp at or below one an
-- earlier release already used.
--
-- The seed appears exactly once and the tracker once per hop, so no release
-- rewrote, duplicated or dropped the entries it inherited, and the newest entry
-- is this hop's own append, stamped with the binary that made it.
SHOW CHANGES FOR TABLE chain_cf_src SINCE 0 LIMIT 1000;
