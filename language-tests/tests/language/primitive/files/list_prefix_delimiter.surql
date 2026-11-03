/**
[env]
namespace = true
database = true

[test]
reason = "file::list matches `prefix` as a plain string, keeps a trailing slash, groups on `delimiter`, pages through groups with `start`, and reports each object's etag."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['/a.txt', '/b.txt', '/b/c.txt', '/b/d/e.txt', '/bz.txt', '/u/tobie/x.png']"

[[test.results]]
value = "['/b.txt', '/b/c.txt', '/b/d/e.txt', '/bz.txt']"

[[test.results]]
value = "['/b/c.txt', '/b/d/e.txt']"

[[test.results]]
value = "[{ prefix: '/b/' }, { prefix: '/u/' }]"

[[test.results]]
value = "['/b/c.txt', '/b/d/']"

[[test.results]]
value = "['/u/tobie/']"

[[test.results]]
value = "['/a.txt', '/b.txt', '/b/']"

[[test.results]]
value = "['/bz.txt', '/u/']"

[[test.results]]
value = "['/b/c.txt', '/b/d/e.txt']"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
error = 'Incorrect arguments for function file::list(). The `delimiter` option must be "/", not ","'

[[test.results]]
error = "Incorrect arguments for function file::list(). The `prefix` option cannot contain a `.` or `..` segment"

*/

DEFINE BUCKET b BACKEND "memory";
FOR $k IN ["/a.txt", "/b.txt", "/b/c.txt", "/b/d/e.txt", "/bz.txt", "/u/tobie/x.png"] {
	file::put(type::file("b", $k), "x");
};
LET $keys = |$opts| file::list("b", $opts).map(|$e| $e.prefix ?? file::key($e.file));

-- Recursive by default.
$keys({});
-- A string prefix: `/b` reaches `/b.txt` and `/bz.txt` as well as `/b/...`.
$keys({ prefix: "/b" });
-- A trailing slash is kept, so `/b/` reaches only what is inside `/b/`.
$keys({ prefix: "/b/" });
-- One level, with each sub-folder reported once as `{ prefix }`.
file::list("b", { delimiter: "/" }).filter(|$e| $e.prefix != NONE);
$keys({ prefix: "/b/", delimiter: "/" });
-- A prefix may end mid-name.
$keys({ prefix: "/u/to", delimiter: "/" });
-- Paging: the last entry of a page is the `start` of the next, a group included.
$keys({ delimiter: "/", limit: 3 });
$keys({ delimiter: "/", start: "/b/", limit: 3 });
-- `start` also takes the `file` of an entry as it is.
$keys({ start: file::list("b", { limit: 2 })[1].file, limit: 2 });
-- Each object carries the etag `file::head` reports.
file::list("b", { prefix: "/a.txt" })[0].etag = file::head(f"b:/a.txt").etag;
type::is_string(file::head(f"b:/a.txt").etag);
-- Only "/" can be a delimiter.
file::list("b", { delimiter: "," });
-- A prefix cannot climb out of the bucket.
file::list("b", { prefix: "/b/../a" });
