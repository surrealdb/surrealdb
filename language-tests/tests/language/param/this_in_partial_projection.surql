/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A bare $this or $self in a projection that does not keep the whole row is the current record, not NONE"

[[test.results]]
value = "[{ id: t:one, n: 1 }]"

[[test.results]]
value = "[{ me: { id: t:one, n: 1 }, n: 1 }]"

[[test.results]]
value = "[{ me: { id: t:one, n: 1 } }]"

[[test.results]]
value = "[{ me: { id: t:one, n: 1 } }]"

[[test.results]]
value = "[{ me: [{ id: t:one, n: 1 }] }]"

[[test.results]]
value = "[{ me: { id: t:one, n: 1 }, n: 1 }]"

[[test.results]]
value = "[{ me: { id: t:one, n: 1 }, n: 1 }]"

*/

CREATE t:one SET n = 1;
SELECT $this AS me, n FROM t;
SELECT $this AS me FROM t;
SELECT $self AS me FROM t;
SELECT [$this] AS me FROM t;
SELECT $this AS me, n FROM t WHERE n = 1;
SELECT $this AS me, n FROM t ORDER BY n;
