/**
[env]
namespace = true
database = true
imports = ["language/indexes/spatial/spatial_select_permission_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]

[[test.results]]
value = "user:alice"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[places:alice_uk1, places:alice_uk2]"

[[test.results]]
value = "[places:alice_uk1, places:alice_uk2]"

[[test.results]]
value = "[{ hidden_loc: NONE, id: places:alice_uk1, label: 'Westminster', secret: NONE }, { hidden_loc: NONE, id: places:alice_uk2, label: 'Greenwich', secret: NONE }]"

[[test.results]]
value = "[{ hidden_loc: NONE, id: places:alice_uk1, label: 'Westminster', secret: NONE }, { hidden_loc: NONE, id: places:alice_uk2, label: 'Greenwich', secret: NONE }]"

[[test.results]]
value = "[{ id: places:alice_uk1, label: 'Westminster', loc: (-0.12f, 51.51f), owner: user:alice }, { id: places:alice_uk2, label: 'Greenwich', loc: (0f, 51.48f), owner: user:alice }]"

[[test.results]]
value = "[{ id: places:alice_uk1, label: 'Westminster', loc: (-0.12f, 51.51f), owner: user:alice }, { id: places:alice_uk2, label: 'Greenwich', loc: (0f, 51.48f), owner: user:alice }]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[zones:alice_uk]"

[[test.results]]
value = "[zones:alice_uk]"
*/

RETURN $auth;
LET $uk_box = {
	type: "Polygon",
	coordinates: [[[-1.0, 51.0], [0.5, 51.0], [0.5, 52.0], [-1.0, 52.0], [-1.0, 51.0]]]
};

-- 1. Key-only `SELECT VALUE id` as record user enforces table SELECT permission
SELECT VALUE id FROM places WHERE loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;

-- 2. Projected fields (`SELECT id, label, secret, hidden_loc`) enforce both table and field SELECT permissions
SELECT id, label, secret, hidden_loc FROM places WHERE loc INSIDE $uk_box ORDER BY id;
SELECT id, label, secret, hidden_loc FROM places WITH NOINDEX WHERE loc INSIDE $uk_box ORDER BY id;

-- 3. Full-row `SELECT *` with `geo::distance` enforces table and field SELECT permissions
SELECT * FROM places WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 ORDER BY id;
SELECT * FROM places WITH NOINDEX WHERE geo::distance(loc, (-0.12, 51.51)) < 50000.0 ORDER BY id;

-- 4. `SELECT count() ... GROUP ALL` as record user counts only visible rows
SELECT VALUE count() FROM places WHERE loc INSIDE $uk_box GROUP ALL;
SELECT VALUE count() FROM places WITH NOINDEX WHERE loc INSIDE $uk_box GROUP ALL;

-- 5. Spatial index on a field with `PERMISSIONS FOR select NONE` (`hidden_loc`) does not leak rows through WHERE
SELECT VALUE id FROM places WHERE hidden_loc INSIDE $uk_box ORDER BY id;
SELECT VALUE id FROM places WITH NOINDEX WHERE hidden_loc INSIDE $uk_box ORDER BY id;

-- 6. Polygon-containing SPATIAL index (`zones`) with `id`-referencing table SELECT permission
SELECT VALUE id FROM zones WHERE geom CONTAINS (-0.12, 51.51) ORDER BY id;
SELECT VALUE id FROM zones WITH NOINDEX WHERE geom CONTAINS (-0.12, 51.51) ORDER BY id;
