/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 30000

[test]
reason = """
A count over an indexed column declared a scalar kind is answered from the
index keys alone. The first batch of keys counts a range that holds fewer, and
the store counts what remains of a range that fills it; around the batch
boundary and over the whole index, the count equals a table scan's, for a
non-unique and a UNIQUE index."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[999, 999]"

[[test.results]]
value = "[1000, 1000]"

[[test.results]]
value = "[1001, 1001]"

[[test.results]]
value = "[2100, 2100]"

[[test.results]]
value = "[999, 999]"

[[test.results]]
value = "[1000, 1000]"

[[test.results]]
value = "[1001, 1001]"

[[test.results]]
value = "[2100, 2100]"

*/

DEFINE TABLE c SCHEMALESS;
DEFINE FIELD n ON c TYPE int;
DEFINE INDEX c_n ON c FIELDS n;
DEFINE TABLE cu SCHEMALESS;
DEFINE FIELD n ON cu TYPE int;
DEFINE INDEX cu_n ON cu FIELDS n UNIQUE;
FOR $i IN 0..2100 { CREATE type::record('c', $i) SET n = $i; };
FOR $i IN 0..2100 { CREATE type::record('cu', $i) SET n = $i; };

-- non-unique index `c_n`: one short of a batch, a full batch, one past it, and
-- every entry; each against the count a table scan gives.
[(SELECT count() FROM c WHERE n < 999 GROUP ALL)[0].count,
	(SELECT count() FROM c WITH NOINDEX WHERE n < 999 GROUP ALL)[0].count];
[(SELECT count() FROM c WHERE n < 1000 GROUP ALL)[0].count,
	(SELECT count() FROM c WITH NOINDEX WHERE n < 1000 GROUP ALL)[0].count];
[(SELECT count() FROM c WHERE n < 1001 GROUP ALL)[0].count,
	(SELECT count() FROM c WITH NOINDEX WHERE n < 1001 GROUP ALL)[0].count];
[(SELECT count() FROM c WHERE n >= 0 GROUP ALL)[0].count,
	(SELECT count() FROM c WITH NOINDEX WHERE n >= 0 GROUP ALL)[0].count];

-- UNIQUE index `cu_n`: one short of a batch, a full batch, one past it, and
-- every entry; each against the count a table scan gives.
[(SELECT count() FROM cu WHERE n < 999 GROUP ALL)[0].count,
	(SELECT count() FROM cu WITH NOINDEX WHERE n < 999 GROUP ALL)[0].count];
[(SELECT count() FROM cu WHERE n < 1000 GROUP ALL)[0].count,
	(SELECT count() FROM cu WITH NOINDEX WHERE n < 1000 GROUP ALL)[0].count];
[(SELECT count() FROM cu WHERE n < 1001 GROUP ALL)[0].count,
	(SELECT count() FROM cu WITH NOINDEX WHERE n < 1001 GROUP ALL)[0].count];
[(SELECT count() FROM cu WHERE n >= 0 GROUP ALL)[0].count,
	(SELECT count() FROM cu WITH NOINDEX WHERE n >= 0 GROUP ALL)[0].count];
