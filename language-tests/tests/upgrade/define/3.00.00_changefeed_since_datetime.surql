/**
[env]
imports = ["upgrade/define/3.00.00_changefeed_import.surql"]

[test]
upgrade = true
reason = """
A datetime finds the change sets an older release wrote to the feed.

`SHOW CHANGES ... SINCE <datetime>` converts the instant into the stamps the
feed is keyed by and scans from there, so the conversion has to land in the
unit the writer minted its stamps in, not in the unit of the backend's own
version clock. A conversion in any other unit starts the scan decades or
centuries away from the entries: short of them it returns the whole feed for
any instant, past them it returns none. 2020 precedes every write the import
makes and 2100 follows them all, so the two reads pin both directions. The read
from versionstamp 0 is the control: it shows the entries are there to find.

`version` bounds the target to the checkout whose conversion lands in the
writer's unit on every backend, so the pairwise hops between older releases
skip it; the seed axis sends every released version straight to the current
checkout, which is the hop that matters. A backport must lower that bound to
its own line or the test silently never runs there.
"""
version = ">=3.4.0-a"

[[test.results]]
match = '''
    type::is_array($result)
    && array::flatten($result.changes)[WHERE $this.update.id == cf_src:1].update.v == [1, 2]
'''

[[test.results]]
match = '''
    type::is_array($result)
    && array::flatten($result.changes)[WHERE $this.update.id == cf_src:1].update.v == [1, 2]
'''

[[test.results]]
value = "[]"

*/
SHOW CHANGES FOR TABLE cf_src SINCE 0 LIMIT 100;
SHOW CHANGES FOR TABLE cf_src SINCE d'2020-01-01T00:00:00Z' LIMIT 100;
SHOW CHANGES FOR TABLE cf_src SINCE d'2100-01-01T00:00:00Z' LIMIT 100;
