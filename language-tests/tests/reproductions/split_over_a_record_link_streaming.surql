/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Half of a pair. Same queries as
`split_over_a_record_link_legacy.surql`, pinned to the streaming
planner; the two files' expected results are the divergence.

A `SPLIT` target is read at the position it names, without dereferencing, so
`link.tags` over `link: cfg:one` is not the linked record's array and the row
does not split. Both engines agree on that, and so on the row count.

They differ in what the row then holds. The streaming engine writes back only
what it split, so a target that named nothing leaves the row as it stands and
`link` remains the record link it was. Recorded so that a change to the
boundary is visible, not as a statement that either answer is the settled one.
"""

[[test.results]]
value = "[{ id: cfg:one, tags: [1, 2] }]"

[[test.results]]
value = "[{ id: t:1, link: cfg:one }]"

[[test.results]]
value = "[{ id: t:1, link: cfg:one }]"

[[test.results]]
value = "[{ id: t:1, tags: 1 }, { id: t:1, tags: 2 }]"
*/

CREATE cfg:one SET tags = [1, 2];
CREATE t:1 SET link = cfg:one;

-- The target crosses a record link, so it names nothing on the row.
SELECT * FROM t SPLIT link.tags;

-- The control: a target that names a collection on the row splits it.
SELECT * FROM (SELECT id, link.tags AS tags FROM t) SPLIT tags;
