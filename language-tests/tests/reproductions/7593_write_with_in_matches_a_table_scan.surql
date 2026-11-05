/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 10000

[test]
reason = "IN, INSIDE and OR of equalities on the leading field of a compound index, UNIQUE or not, select, count, page, update and delete the rows a table scan of the same predicate does. Each oracle check also requires its plan to read an index, and each oracle statement lists the checks that do not hold."
issue = 7593

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

LET $seed = [
	{ id: item:a, group: 'g1', position: 0, tag: 'x' },
	{ id: item:b, group: 'g1', position: 1, tag: 'y' },
	{ id: item:c, group: 'g2', position: 0, tag: 'x' },
	{ id: item:d, group: 'g3', position: 0, tag: 'z' },
	{ id: item:e, group: 'g2', position: 1 }
];

-- Every predicate matches at least two rows, so each paged read drops a row
-- and keeps another.
INSERT INTO item $seed RETURN NONE;
RETURN [
	{ q: "group IN ['g1', 'g2']", n: array::len(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2']) },
	{ q: "group IN ['g2']", n: array::len(SELECT VALUE id FROM item WHERE group IN ['g2']) },
	{ q: "group INSIDE ['g1']", n: array::len(SELECT VALUE id FROM item WHERE group INSIDE ['g1']) },
	{ q: "group IN ['g1', 'missing']", n: array::len(SELECT VALUE id FROM item WHERE group IN ['g1', 'missing']) },
	{ q: "group = 'g1' OR group = 'g2'", n: array::len(SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2') },
	{ q: "group IN ['g1', 'g2'] AND position = 0", n: array::len(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0) },
	{ q: "group IN ['g1', 'g2'] AND position IN [1]", n: array::len(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1]) },
	{ q: "group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", n: array::len(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x') },
	{ q: "group IN ['g1', 'g2'] AND position > 0", n: array::len(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0) }
].filter(|$c| $c.n < 2).map(|$c| $c.q);

-- Reads, updates and deletes are checked in separate statements, since a
-- statement that writes plans its reads for writing. Each delete check puts the
-- seed back before the next.
REMOVE TABLE IF EXISTS item; DEFINE TABLE item SCHEMALESS; DEFINE INDEX ix ON item FIELDS group, position UNIQUE;
INSERT INTO item $seed RETURN NONE;
RETURN [
	{ q: "compound UNIQUE select group IN ['g1', 'g2']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'])) },
	{ q: "compound UNIQUE select group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2']).contains('Index') },
	{ q: "compound UNIQUE page group IN ['g1', 'g2']", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound UNIQUE page group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound UNIQUE count group IN ['g1', 'g2']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] GROUP ALL) },
	{ q: "compound UNIQUE count group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] GROUP ALL).contains('Index') },
	{ q: "compound UNIQUE select group IN ['g2']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g2'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'])) },
	{ q: "compound UNIQUE select group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g2']).contains('Index') },
	{ q: "compound UNIQUE page group IN ['g2']", ok: (SELECT VALUE id FROM item WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound UNIQUE page group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound UNIQUE count group IN ['g2']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g2'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g2'] GROUP ALL) },
	{ q: "compound UNIQUE count group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g2'] GROUP ALL).contains('Index') },
	{ q: "compound UNIQUE select group INSIDE ['g1']", ok: (array::sort(SELECT VALUE id FROM item WHERE group INSIDE ['g1'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'])) },
	{ q: "compound UNIQUE select group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group INSIDE ['g1']).contains('Index') },
	{ q: "compound UNIQUE page group INSIDE ['g1']", ok: (SELECT VALUE id FROM item WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound UNIQUE page group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound UNIQUE count group INSIDE ['g1']", ok: (SELECT VALUE count() FROM item WHERE group INSIDE ['g1'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group INSIDE ['g1'] GROUP ALL) },
	{ q: "compound UNIQUE count group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group INSIDE ['g1'] GROUP ALL).contains('Index') },
	{ q: "compound UNIQUE select group IN ['g1', 'missing']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'])) },
	{ q: "compound UNIQUE select group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'missing']).contains('Index') },
	{ q: "compound UNIQUE page group IN ['g1', 'missing']", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound UNIQUE page group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound UNIQUE count group IN ['g1', 'missing']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'missing'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'] GROUP ALL) },
	{ q: "compound UNIQUE count group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'missing'] GROUP ALL).contains('Index') },
	{ q: "compound UNIQUE select group = 'g1' OR group = 'g2'", ok: (array::sort(SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2')) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2')) },
	{ q: "compound UNIQUE select group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2').contains('Index') },
	{ q: "compound UNIQUE page group = 'g1' OR group = 'g2'", ok: (SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound UNIQUE page group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound UNIQUE count group = 'g1' OR group = 'g2'", ok: (SELECT VALUE count() FROM item WHERE group = 'g1' OR group = 'g2' GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2' GROUP ALL) },
	{ q: "compound UNIQUE count group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group = 'g1' OR group = 'g2' GROUP ALL).contains('Index') },
	{ q: "compound UNIQUE select group IN ['g1', 'g2'] AND position = 0", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0)) },
	{ q: "compound UNIQUE select group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0).contains('Index') },
	{ q: "compound UNIQUE page group IN ['g1', 'g2'] AND position = 0", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound UNIQUE page group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound UNIQUE count group IN ['g1', 'g2'] AND position = 0", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL) },
	{ q: "compound UNIQUE count group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL).contains('Index') },
	{ q: "compound UNIQUE select group IN ['g1', 'g2'] AND position IN [1]", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1])) },
	{ q: "compound UNIQUE select group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1]).contains('Index') },
	{ q: "compound UNIQUE page group IN ['g1', 'g2'] AND position IN [1]", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound UNIQUE page group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound UNIQUE count group IN ['g1', 'g2'] AND position IN [1]", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL) },
	{ q: "compound UNIQUE count group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL).contains('Index') },
	{ q: "compound UNIQUE select group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) },
	{ q: "compound UNIQUE select group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x').contains('Index') },
	{ q: "compound UNIQUE page group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound UNIQUE page group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound UNIQUE count group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL) },
	{ q: "compound UNIQUE count group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL).contains('Index') },
	{ q: "compound UNIQUE select group IN ['g1', 'g2'] AND position > 0", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0)) },
	{ q: "compound UNIQUE select group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0).contains('Index') },
	{ q: "compound UNIQUE page group IN ['g1', 'g2'] AND position > 0", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound UNIQUE page group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound UNIQUE count group IN ['g1', 'g2'] AND position > 0", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL) },
	{ q: "compound UNIQUE count group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
RETURN [
	{ q: "compound UNIQUE update group IN ['g1', 'g2']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'])) },
	{ q: "compound UNIQUE update group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] RETURN VALUE id).contains('Index') },
	{ q: "compound UNIQUE update group IN ['g2']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g2'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'])) },
	{ q: "compound UNIQUE update group IN ['g2'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g2'] RETURN VALUE id).contains('Index') },
	{ q: "compound UNIQUE update group INSIDE ['g1']", ok: (array::sort(UPDATE item SET touched = true WHERE group INSIDE ['g1'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'])) },
	{ q: "compound UNIQUE update group INSIDE ['g1'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group INSIDE ['g1'] RETURN VALUE id).contains('Index') },
	{ q: "compound UNIQUE update group IN ['g1', 'missing']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'missing'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'])) },
	{ q: "compound UNIQUE update group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'missing'] RETURN VALUE id).contains('Index') },
	{ q: "compound UNIQUE update group = 'g1' OR group = 'g2'", ok: (array::sort(UPDATE item SET touched = true WHERE group = 'g1' OR group = 'g2' RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2')) },
	{ q: "compound UNIQUE update group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group = 'g1' OR group = 'g2' RETURN VALUE id).contains('Index') },
	{ q: "compound UNIQUE update group IN ['g1', 'g2'] AND position = 0", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0)) },
	{ q: "compound UNIQUE update group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE id).contains('Index') },
	{ q: "compound UNIQUE update group IN ['g1', 'g2'] AND position IN [1]", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1])) },
	{ q: "compound UNIQUE update group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE id).contains('Index') },
	{ q: "compound UNIQUE update group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) },
	{ q: "compound UNIQUE update group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE id).contains('Index') },
	{ q: "compound UNIQUE update group IN ['g1', 'g2'] AND position > 0", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0)) },
	{ q: "compound UNIQUE update group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE id).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
RETURN [
	{ q: "compound UNIQUE delete group IN ['g1', 'g2']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2']); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound UNIQUE delete group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2']).contains('Index') },
	{ q: "compound UNIQUE delete group IN ['g2']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2']); LET $got = array::sort(DELETE item WHERE group IN ['g2'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound UNIQUE delete group IN ['g2'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g2']).contains('Index') },
	{ q: "compound UNIQUE delete group INSIDE ['g1']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1']); LET $got = array::sort(DELETE item WHERE group INSIDE ['g1'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound UNIQUE delete group INSIDE ['g1'] (plan)", ok: (EXPLAIN DELETE item WHERE group INSIDE ['g1']).contains('Index') },
	{ q: "compound UNIQUE delete group IN ['g1', 'missing']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing']); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'missing'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound UNIQUE delete group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'missing']).contains('Index') },
	{ q: "compound UNIQUE delete group = 'g1' OR group = 'g2'", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2'); LET $got = array::sort(DELETE item WHERE group = 'g1' OR group = 'g2' RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound UNIQUE delete group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN DELETE item WHERE group = 'g1' OR group = 'g2').contains('Index') },
	{ q: "compound UNIQUE delete group IN ['g1', 'g2'] AND position = 0", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound UNIQUE delete group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position = 0).contains('Index') },
	{ q: "compound UNIQUE delete group IN ['g1', 'g2'] AND position IN [1]", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1]); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound UNIQUE delete group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position IN [1]).contains('Index') },
	{ q: "compound UNIQUE delete group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound UNIQUE delete group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x').contains('Index') },
	{ q: "compound UNIQUE delete group IN ['g1', 'g2'] AND position > 0", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound UNIQUE delete group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position > 0).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
REMOVE TABLE IF EXISTS item; DEFINE TABLE item SCHEMALESS; DEFINE INDEX ix ON item FIELDS group, position, tag UNIQUE;
INSERT INTO item $seed RETURN NONE;
RETURN [
	{ q: "three-column UNIQUE select group IN ['g1', 'g2']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'])) },
	{ q: "three-column UNIQUE select group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2']).contains('Index') },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2']", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] GROUP ALL) },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] GROUP ALL).contains('Index') },
	{ q: "three-column UNIQUE select group IN ['g2']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g2'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'])) },
	{ q: "three-column UNIQUE select group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g2']).contains('Index') },
	{ q: "three-column UNIQUE page group IN ['g2']", ok: (SELECT VALUE id FROM item WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "three-column UNIQUE page group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "three-column UNIQUE count group IN ['g2']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g2'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g2'] GROUP ALL) },
	{ q: "three-column UNIQUE count group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g2'] GROUP ALL).contains('Index') },
	{ q: "three-column UNIQUE select group INSIDE ['g1']", ok: (array::sort(SELECT VALUE id FROM item WHERE group INSIDE ['g1'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'])) },
	{ q: "three-column UNIQUE select group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group INSIDE ['g1']).contains('Index') },
	{ q: "three-column UNIQUE page group INSIDE ['g1']", ok: (SELECT VALUE id FROM item WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "three-column UNIQUE page group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "three-column UNIQUE count group INSIDE ['g1']", ok: (SELECT VALUE count() FROM item WHERE group INSIDE ['g1'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group INSIDE ['g1'] GROUP ALL) },
	{ q: "three-column UNIQUE count group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group INSIDE ['g1'] GROUP ALL).contains('Index') },
	{ q: "three-column UNIQUE select group IN ['g1', 'missing']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'])) },
	{ q: "three-column UNIQUE select group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'missing']).contains('Index') },
	{ q: "three-column UNIQUE page group IN ['g1', 'missing']", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "three-column UNIQUE page group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "three-column UNIQUE count group IN ['g1', 'missing']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'missing'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'] GROUP ALL) },
	{ q: "three-column UNIQUE count group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'missing'] GROUP ALL).contains('Index') },
	{ q: "three-column UNIQUE select group = 'g1' OR group = 'g2'", ok: (array::sort(SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2')) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2')) },
	{ q: "three-column UNIQUE select group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2').contains('Index') },
	{ q: "three-column UNIQUE page group = 'g1' OR group = 'g2'", ok: (SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "three-column UNIQUE page group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "three-column UNIQUE count group = 'g1' OR group = 'g2'", ok: (SELECT VALUE count() FROM item WHERE group = 'g1' OR group = 'g2' GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2' GROUP ALL) },
	{ q: "three-column UNIQUE count group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group = 'g1' OR group = 'g2' GROUP ALL).contains('Index') },
	{ q: "three-column UNIQUE select group IN ['g1', 'g2'] AND position = 0", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0)) },
	{ q: "three-column UNIQUE select group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0).contains('Index') },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2'] AND position = 0", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2'] AND position = 0", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL) },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL).contains('Index') },
	{ q: "three-column UNIQUE select group IN ['g1', 'g2'] AND position IN [1]", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1])) },
	{ q: "three-column UNIQUE select group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1]).contains('Index') },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2'] AND position IN [1]", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2'] AND position IN [1]", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL) },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL).contains('Index') },
	{ q: "three-column UNIQUE select group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) },
	{ q: "three-column UNIQUE select group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x').contains('Index') },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL) },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL).contains('Index') },
	{ q: "three-column UNIQUE select group IN ['g1', 'g2'] AND position > 0", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0)) },
	{ q: "three-column UNIQUE select group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0).contains('Index') },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2'] AND position > 0", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "three-column UNIQUE page group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2'] AND position > 0", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL) },
	{ q: "three-column UNIQUE count group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
RETURN [
	{ q: "three-column UNIQUE update group IN ['g1', 'g2']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'])) },
	{ q: "three-column UNIQUE update group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] RETURN VALUE id).contains('Index') },
	{ q: "three-column UNIQUE update group IN ['g2']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g2'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'])) },
	{ q: "three-column UNIQUE update group IN ['g2'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g2'] RETURN VALUE id).contains('Index') },
	{ q: "three-column UNIQUE update group INSIDE ['g1']", ok: (array::sort(UPDATE item SET touched = true WHERE group INSIDE ['g1'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'])) },
	{ q: "three-column UNIQUE update group INSIDE ['g1'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group INSIDE ['g1'] RETURN VALUE id).contains('Index') },
	{ q: "three-column UNIQUE update group IN ['g1', 'missing']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'missing'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'])) },
	{ q: "three-column UNIQUE update group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'missing'] RETURN VALUE id).contains('Index') },
	{ q: "three-column UNIQUE update group = 'g1' OR group = 'g2'", ok: (array::sort(UPDATE item SET touched = true WHERE group = 'g1' OR group = 'g2' RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2')) },
	{ q: "three-column UNIQUE update group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group = 'g1' OR group = 'g2' RETURN VALUE id).contains('Index') },
	{ q: "three-column UNIQUE update group IN ['g1', 'g2'] AND position = 0", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0)) },
	{ q: "three-column UNIQUE update group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE id).contains('Index') },
	{ q: "three-column UNIQUE update group IN ['g1', 'g2'] AND position IN [1]", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1])) },
	{ q: "three-column UNIQUE update group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE id).contains('Index') },
	{ q: "three-column UNIQUE update group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) },
	{ q: "three-column UNIQUE update group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE id).contains('Index') },
	{ q: "three-column UNIQUE update group IN ['g1', 'g2'] AND position > 0", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0)) },
	{ q: "three-column UNIQUE update group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE id).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
RETURN [
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2']); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2']).contains('Index') },
	{ q: "three-column UNIQUE delete group IN ['g2']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2']); LET $got = array::sort(DELETE item WHERE group IN ['g2'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "three-column UNIQUE delete group IN ['g2'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g2']).contains('Index') },
	{ q: "three-column UNIQUE delete group INSIDE ['g1']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1']); LET $got = array::sort(DELETE item WHERE group INSIDE ['g1'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "three-column UNIQUE delete group INSIDE ['g1'] (plan)", ok: (EXPLAIN DELETE item WHERE group INSIDE ['g1']).contains('Index') },
	{ q: "three-column UNIQUE delete group IN ['g1', 'missing']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing']); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'missing'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "three-column UNIQUE delete group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'missing']).contains('Index') },
	{ q: "three-column UNIQUE delete group = 'g1' OR group = 'g2'", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2'); LET $got = array::sort(DELETE item WHERE group = 'g1' OR group = 'g2' RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "three-column UNIQUE delete group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN DELETE item WHERE group = 'g1' OR group = 'g2').contains('Index') },
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2'] AND position = 0", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position = 0).contains('Index') },
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2'] AND position IN [1]", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1]); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position IN [1]).contains('Index') },
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x').contains('Index') },
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2'] AND position > 0", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "three-column UNIQUE delete group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position > 0).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
REMOVE TABLE IF EXISTS item; DEFINE TABLE item SCHEMALESS; DEFINE INDEX ix ON item FIELDS group, position;
INSERT INTO item $seed RETURN NONE;
RETURN [
	{ q: "compound select group IN ['g1', 'g2']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'])) },
	{ q: "compound select group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2']).contains('Index') },
	{ q: "compound page group IN ['g1', 'g2']", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound page group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound count group IN ['g1', 'g2']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] GROUP ALL) },
	{ q: "compound count group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] GROUP ALL).contains('Index') },
	{ q: "compound select group IN ['g2']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g2'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'])) },
	{ q: "compound select group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g2']).contains('Index') },
	{ q: "compound page group IN ['g2']", ok: (SELECT VALUE id FROM item WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound page group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound count group IN ['g2']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g2'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g2'] GROUP ALL) },
	{ q: "compound count group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g2'] GROUP ALL).contains('Index') },
	{ q: "compound select group INSIDE ['g1']", ok: (array::sort(SELECT VALUE id FROM item WHERE group INSIDE ['g1'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'])) },
	{ q: "compound select group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group INSIDE ['g1']).contains('Index') },
	{ q: "compound page group INSIDE ['g1']", ok: (SELECT VALUE id FROM item WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound page group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound count group INSIDE ['g1']", ok: (SELECT VALUE count() FROM item WHERE group INSIDE ['g1'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group INSIDE ['g1'] GROUP ALL) },
	{ q: "compound count group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group INSIDE ['g1'] GROUP ALL).contains('Index') },
	{ q: "compound select group IN ['g1', 'missing']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'])) },
	{ q: "compound select group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'missing']).contains('Index') },
	{ q: "compound page group IN ['g1', 'missing']", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound page group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound count group IN ['g1', 'missing']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'missing'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'] GROUP ALL) },
	{ q: "compound count group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'missing'] GROUP ALL).contains('Index') },
	{ q: "compound select group = 'g1' OR group = 'g2'", ok: (array::sort(SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2')) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2')) },
	{ q: "compound select group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2').contains('Index') },
	{ q: "compound page group = 'g1' OR group = 'g2'", ok: (SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound page group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound count group = 'g1' OR group = 'g2'", ok: (SELECT VALUE count() FROM item WHERE group = 'g1' OR group = 'g2' GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2' GROUP ALL) },
	{ q: "compound count group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group = 'g1' OR group = 'g2' GROUP ALL).contains('Index') },
	{ q: "compound select group IN ['g1', 'g2'] AND position = 0", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0)) },
	{ q: "compound select group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0).contains('Index') },
	{ q: "compound page group IN ['g1', 'g2'] AND position = 0", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound page group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound count group IN ['g1', 'g2'] AND position = 0", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL) },
	{ q: "compound count group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL).contains('Index') },
	{ q: "compound select group IN ['g1', 'g2'] AND position IN [1]", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1])) },
	{ q: "compound select group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1]).contains('Index') },
	{ q: "compound page group IN ['g1', 'g2'] AND position IN [1]", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound page group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound count group IN ['g1', 'g2'] AND position IN [1]", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL) },
	{ q: "compound count group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL).contains('Index') },
	{ q: "compound select group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) },
	{ q: "compound select group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x').contains('Index') },
	{ q: "compound page group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound page group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound count group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL) },
	{ q: "compound count group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL).contains('Index') },
	{ q: "compound select group IN ['g1', 'g2'] AND position > 0", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0)) },
	{ q: "compound select group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0).contains('Index') },
	{ q: "compound page group IN ['g1', 'g2'] AND position > 0", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "compound page group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "compound count group IN ['g1', 'g2'] AND position > 0", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL) },
	{ q: "compound count group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
RETURN [
	{ q: "compound update group IN ['g1', 'g2']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'])) },
	{ q: "compound update group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] RETURN VALUE id).contains('Index') },
	{ q: "compound update group IN ['g2']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g2'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'])) },
	{ q: "compound update group IN ['g2'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g2'] RETURN VALUE id).contains('Index') },
	{ q: "compound update group INSIDE ['g1']", ok: (array::sort(UPDATE item SET touched = true WHERE group INSIDE ['g1'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'])) },
	{ q: "compound update group INSIDE ['g1'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group INSIDE ['g1'] RETURN VALUE id).contains('Index') },
	{ q: "compound update group IN ['g1', 'missing']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'missing'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'])) },
	{ q: "compound update group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'missing'] RETURN VALUE id).contains('Index') },
	{ q: "compound update group = 'g1' OR group = 'g2'", ok: (array::sort(UPDATE item SET touched = true WHERE group = 'g1' OR group = 'g2' RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2')) },
	{ q: "compound update group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group = 'g1' OR group = 'g2' RETURN VALUE id).contains('Index') },
	{ q: "compound update group IN ['g1', 'g2'] AND position = 0", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0)) },
	{ q: "compound update group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE id).contains('Index') },
	{ q: "compound update group IN ['g1', 'g2'] AND position IN [1]", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1])) },
	{ q: "compound update group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE id).contains('Index') },
	{ q: "compound update group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) },
	{ q: "compound update group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE id).contains('Index') },
	{ q: "compound update group IN ['g1', 'g2'] AND position > 0", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0)) },
	{ q: "compound update group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE id).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
RETURN [
	{ q: "compound delete group IN ['g1', 'g2']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2']); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound delete group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2']).contains('Index') },
	{ q: "compound delete group IN ['g2']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2']); LET $got = array::sort(DELETE item WHERE group IN ['g2'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound delete group IN ['g2'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g2']).contains('Index') },
	{ q: "compound delete group INSIDE ['g1']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1']); LET $got = array::sort(DELETE item WHERE group INSIDE ['g1'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound delete group INSIDE ['g1'] (plan)", ok: (EXPLAIN DELETE item WHERE group INSIDE ['g1']).contains('Index') },
	{ q: "compound delete group IN ['g1', 'missing']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing']); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'missing'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound delete group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'missing']).contains('Index') },
	{ q: "compound delete group = 'g1' OR group = 'g2'", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2'); LET $got = array::sort(DELETE item WHERE group = 'g1' OR group = 'g2' RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound delete group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN DELETE item WHERE group = 'g1' OR group = 'g2').contains('Index') },
	{ q: "compound delete group IN ['g1', 'g2'] AND position = 0", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound delete group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position = 0).contains('Index') },
	{ q: "compound delete group IN ['g1', 'g2'] AND position IN [1]", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1]); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound delete group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position IN [1]).contains('Index') },
	{ q: "compound delete group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound delete group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x').contains('Index') },
	{ q: "compound delete group IN ['g1', 'g2'] AND position > 0", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "compound delete group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position > 0).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
REMOVE TABLE IF EXISTS item; DEFINE TABLE item SCHEMALESS; DEFINE INDEX ix ON item FIELDS group;
INSERT INTO item $seed RETURN NONE;
RETURN [
	{ q: "single select group IN ['g1', 'g2']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'])) },
	{ q: "single select group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2']).contains('Index') },
	{ q: "single page group IN ['g1', 'g2']", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "single page group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "single count group IN ['g1', 'g2']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] GROUP ALL) },
	{ q: "single count group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] GROUP ALL).contains('Index') },
	{ q: "single select group IN ['g2']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g2'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'])) },
	{ q: "single select group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g2']).contains('Index') },
	{ q: "single page group IN ['g2']", ok: (SELECT VALUE id FROM item WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "single page group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g2'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "single count group IN ['g2']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g2'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g2'] GROUP ALL) },
	{ q: "single count group IN ['g2'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g2'] GROUP ALL).contains('Index') },
	{ q: "single select group INSIDE ['g1']", ok: (array::sort(SELECT VALUE id FROM item WHERE group INSIDE ['g1'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'])) },
	{ q: "single select group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group INSIDE ['g1']).contains('Index') },
	{ q: "single page group INSIDE ['g1']", ok: (SELECT VALUE id FROM item WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "single page group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group INSIDE ['g1'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "single count group INSIDE ['g1']", ok: (SELECT VALUE count() FROM item WHERE group INSIDE ['g1'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group INSIDE ['g1'] GROUP ALL) },
	{ q: "single count group INSIDE ['g1'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group INSIDE ['g1'] GROUP ALL).contains('Index') },
	{ q: "single select group IN ['g1', 'missing']", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'])) },
	{ q: "single select group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'missing']).contains('Index') },
	{ q: "single page group IN ['g1', 'missing']", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "single page group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'missing'] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "single count group IN ['g1', 'missing']", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'missing'] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'] GROUP ALL) },
	{ q: "single count group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'missing'] GROUP ALL).contains('Index') },
	{ q: "single select group = 'g1' OR group = 'g2'", ok: (array::sort(SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2')) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2')) },
	{ q: "single select group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2').contains('Index') },
	{ q: "single page group = 'g1' OR group = 'g2'", ok: (SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "single page group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group = 'g1' OR group = 'g2' ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "single count group = 'g1' OR group = 'g2'", ok: (SELECT VALUE count() FROM item WHERE group = 'g1' OR group = 'g2' GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2' GROUP ALL) },
	{ q: "single count group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group = 'g1' OR group = 'g2' GROUP ALL).contains('Index') },
	{ q: "single select group IN ['g1', 'g2'] AND position = 0", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0)) },
	{ q: "single select group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0).contains('Index') },
	{ q: "single page group IN ['g1', 'g2'] AND position = 0", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "single page group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "single count group IN ['g1', 'g2'] AND position = 0", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL) },
	{ q: "single count group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 GROUP ALL).contains('Index') },
	{ q: "single select group IN ['g1', 'g2'] AND position IN [1]", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1])) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1])) },
	{ q: "single select group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1]).contains('Index') },
	{ q: "single page group IN ['g1', 'g2'] AND position IN [1]", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "single page group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "single count group IN ['g1', 'g2'] AND position IN [1]", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL) },
	{ q: "single count group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position IN [1] GROUP ALL).contains('Index') },
	{ q: "single select group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) },
	{ q: "single select group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x').contains('Index') },
	{ q: "single page group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "single page group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "single count group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL) },
	{ q: "single count group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' GROUP ALL).contains('Index') },
	{ q: "single select group IN ['g1', 'g2'] AND position > 0", ok: (array::sort(SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0)) },
	{ q: "single select group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0).contains('Index') },
	{ q: "single page group IN ['g1', 'g2'] AND position > 0", ok: (SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) == (SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1) },
	{ q: "single page group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'] AND position > 0 ORDER BY group DESC, id DESC LIMIT 1 START 1).contains('Index') },
	{ q: "single count group IN ['g1', 'g2'] AND position > 0", ok: (SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL) == (SELECT VALUE count() FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL) },
	{ q: "single count group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN SELECT VALUE count() FROM item WHERE group IN ['g1', 'g2'] AND position > 0 GROUP ALL).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
RETURN [
	{ q: "single update group IN ['g1', 'g2']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'])) },
	{ q: "single update group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] RETURN VALUE id).contains('Index') },
	{ q: "single update group IN ['g2']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g2'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2'])) },
	{ q: "single update group IN ['g2'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g2'] RETURN VALUE id).contains('Index') },
	{ q: "single update group INSIDE ['g1']", ok: (array::sort(UPDATE item SET touched = true WHERE group INSIDE ['g1'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1'])) },
	{ q: "single update group INSIDE ['g1'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group INSIDE ['g1'] RETURN VALUE id).contains('Index') },
	{ q: "single update group IN ['g1', 'missing']", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'missing'] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing'])) },
	{ q: "single update group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'missing'] RETURN VALUE id).contains('Index') },
	{ q: "single update group = 'g1' OR group = 'g2'", ok: (array::sort(UPDATE item SET touched = true WHERE group = 'g1' OR group = 'g2' RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2')) },
	{ q: "single update group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group = 'g1' OR group = 'g2' RETURN VALUE id).contains('Index') },
	{ q: "single update group IN ['g1', 'g2'] AND position = 0", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0)) },
	{ q: "single update group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE id).contains('Index') },
	{ q: "single update group IN ['g1', 'g2'] AND position IN [1]", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1])) },
	{ q: "single update group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE id).contains('Index') },
	{ q: "single update group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x')) },
	{ q: "single update group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE id).contains('Index') },
	{ q: "single update group IN ['g1', 'g2'] AND position > 0", ok: (array::sort(UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE id)) == (array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0)) },
	{ q: "single update group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE id).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
RETURN [
	{ q: "single delete group IN ['g1', 'g2']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2']); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "single delete group IN ['g1', 'g2'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2']).contains('Index') },
	{ q: "single delete group IN ['g2']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g2']); LET $got = array::sort(DELETE item WHERE group IN ['g2'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "single delete group IN ['g2'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g2']).contains('Index') },
	{ q: "single delete group INSIDE ['g1']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group INSIDE ['g1']); LET $got = array::sort(DELETE item WHERE group INSIDE ['g1'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "single delete group INSIDE ['g1'] (plan)", ok: (EXPLAIN DELETE item WHERE group INSIDE ['g1']).contains('Index') },
	{ q: "single delete group IN ['g1', 'missing']", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'missing']); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'missing'] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "single delete group IN ['g1', 'missing'] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'missing']).contains('Index') },
	{ q: "single delete group = 'g1' OR group = 'g2'", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group = 'g1' OR group = 'g2'); LET $got = array::sort(DELETE item WHERE group = 'g1' OR group = 'g2' RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "single delete group = 'g1' OR group = 'g2' (plan)", ok: (EXPLAIN DELETE item WHERE group = 'g1' OR group = 'g2').contains('Index') },
	{ q: "single delete group IN ['g1', 'g2'] AND position = 0", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "single delete group IN ['g1', 'g2'] AND position = 0 (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position = 0).contains('Index') },
	{ q: "single delete group IN ['g1', 'g2'] AND position IN [1]", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position IN [1]); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position IN [1] RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "single delete group IN ['g1', 'g2'] AND position IN [1] (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position IN [1]).contains('Index') },
	{ q: "single delete group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x'); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "single delete group IN ['g1', 'g2'] AND position = 0 AND tag = 'x' (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position = 0 AND tag = 'x').contains('Index') },
	{ q: "single delete group IN ['g1', 'g2'] AND position > 0", ok: { LET $want = array::sort(SELECT VALUE id FROM item WITH NOINDEX WHERE group IN ['g1', 'g2'] AND position > 0); LET $got = array::sort(DELETE item WHERE group IN ['g1', 'g2'] AND position > 0 RETURN VALUE $before.id); INSERT IGNORE INTO item $seed RETURN NONE; $got == $want } },
	{ q: "single delete group IN ['g1', 'g2'] AND position > 0 (plan)", ok: (EXPLAIN DELETE item WHERE group IN ['g1', 'g2'] AND position > 0).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
