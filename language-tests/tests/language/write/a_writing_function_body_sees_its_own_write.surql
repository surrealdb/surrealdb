/**
[env]
namespace = true
database = true

[test]
reason = """
A function body that writes reads its own write back. The write and the read
that follows it are statements of one body, so nothing but their order in that
body can sequence them: a call that answered the read from state taken before
its own write would report the record absent.

The second function supplies the read from the mutation itself rather than from
a following statement, and the statement after each call shows the record the
body claimed it wrote is the record the database holds.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['written']"

[[test.results]]
value = "[{ id: note:1, tag: 'written' }]"

[[test.results]]
value = "[{ id: mark:1, n: 1 }]"

[[test.results]]
value = "[{ id: mark:1, n: 1 }]"
*/

DEFINE TABLE note SCHEMALESS;
DEFINE TABLE mark SCHEMALESS;

-- The body writes, then scans for what it wrote.
DEFINE FUNCTION fn::note($tag: string) {
	CREATE note:1 SET tag = $tag;
	RETURN SELECT VALUE tag FROM note;
};

-- The body returns what its own write produced.
DEFINE FUNCTION fn::mark() {
	RETURN CREATE mark:1 SET n = 1;
};

fn::note('written');
SELECT * FROM note;

fn::mark();
SELECT * FROM mark;
