/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for spatial_select_permission.surql: SPATIAL-indexed tables with table-level
-- and field-level SELECT permissions, plus the record user the test signs in as.
{
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 6h;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	CREATE user:bob   SET email = 'bob@surrealdb.com',   pass = crypto::argon2::generate('secret');

	DEFINE TABLE places SCHEMALESS PERMISSIONS
		FOR select WHERE owner = $auth
		FOR create, update, delete NONE;
	DEFINE FIELD secret     ON places VALUE 'classified' PERMISSIONS FOR select NONE;
	DEFINE FIELD loc        ON places TYPE geometry<point>;
	DEFINE FIELD hidden_loc ON places TYPE geometry<point> PERMISSIONS FOR select NONE;
	DEFINE INDEX idx_places_loc        ON places FIELDS loc SPATIAL;
	DEFINE INDEX idx_places_hidden_loc ON places FIELDS hidden_loc SPATIAL;

	INSERT INTO places [
		{ id: places:alice_uk1, owner: user:alice, label: 'Westminster', loc: (-0.12, 51.51), hidden_loc: (-0.12, 51.51) },
		{ id: places:bob_uk1,   owner: user:bob,   label: 'Camden',      loc: (-0.14, 51.54), hidden_loc: (-0.14, 51.54) },
		{ id: places:alice_uk2, owner: user:alice, label: 'Greenwich',   loc: (0.00, 51.48),  hidden_loc: (0.00, 51.48) },
		{ id: places:alice_fr1, owner: user:alice, label: 'Paris',       loc: (2.35, 48.85),  hidden_loc: (2.35, 48.85) },
		{ id: places:bob_fr1,   owner: user:bob,   label: 'Lyon',        loc: (4.83, 45.76),  hidden_loc: (4.83, 45.76) }
	];

	DEFINE TABLE zones SCHEMALESS PERMISSIONS
		FOR select WHERE owner = $auth AND id != zones:hidden_uk
		FOR create, update, delete NONE;
	DEFINE INDEX idx_zones_geom ON zones FIELDS geom SPATIAL;

	INSERT INTO zones [
		{
			id: zones:alice_uk,
			owner: user:alice,
			geom: {
				type: "Polygon",
				coordinates: [[[-1.0, 51.0], [0.5, 51.0], [0.5, 52.0], [-1.0, 52.0], [-1.0, 51.0]]]
			}
		},
		{
			id: zones:hidden_uk,
			owner: user:alice,
			geom: {
				type: "Polygon",
				coordinates: [[[-1.0, 51.0], [0.5, 51.0], [0.5, 52.0], [-1.0, 52.0], [-1.0, 51.0]]]
			}
		},
		{
			id: zones:bob_uk,
			owner: user:bob,
			geom: {
				type: "Polygon",
				coordinates: [[[-1.0, 51.0], [0.5, 51.0], [0.5, 52.0], [-1.0, 52.0], [-1.0, 51.0]]]
			}
		}
	];

	RETURN 'OK';
}
