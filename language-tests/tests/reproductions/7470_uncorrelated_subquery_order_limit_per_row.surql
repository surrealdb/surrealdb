/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An uncorrelated sub-SELECT with ORDER BY and LIMIT must evaluate to the same value for every outer row"
issue = 7470

[[test.results]]
value = "[{ id: orders:o1, status: 'completed', total: 100f }]"

[[test.results]]
value = "[{ id: orders:o2, status: 'pending', total: 50f }]"

[[test.results]]
value = "[{ id: orders:o3, status: 'completed', total: 200f }]"

[[test.results]]
value = "[{ id: orders:o4, status: 'completed', total: 10f }]"

[[test.results]]
value = "[{ id: orders:o5, status: 'pending', total: 20f }]"

[[test.results]]
value = "[[200f], [200f], [200f], [200f], [200f]]"

[[test.results]]
value = "[[200f, 100f, 10f], [200f, 100f, 10f], [200f, 100f, 10f], [200f, 100f, 10f], [200f, 100f, 10f]]"

[[test.results]]
value = "[orders:o3]"
*/

CREATE orders:o1 SET total = 100.0, status = 'completed';
CREATE orders:o2 SET total = 50.0, status = 'pending';
CREATE orders:o3 SET total = 200.0, status = 'completed';
CREATE orders:o4 SET total = 10.0, status = 'completed';
CREATE orders:o5 SET total = 20.0, status = 'pending';
SELECT VALUE (SELECT VALUE total FROM orders WHERE status = 'completed' ORDER BY total DESC LIMIT 1) FROM orders;
SELECT VALUE (SELECT VALUE total FROM orders WHERE status = 'completed' ORDER BY total DESC LIMIT 3) FROM orders;
SELECT VALUE id FROM orders WHERE total = array::first((SELECT VALUE total FROM orders WHERE status = 'completed' ORDER BY total DESC LIMIT 1));
