/**
[env]
namespace = true
database = true

[test]
issue = 1336
reason = """
Records whose ids differ only in a nested number's kind (`pt:[1]` and
`pt:[1dec]`) are two records, and an HNSW or DiskANN index holds each: built
over them, updated one after the other while their writes share a pending key,
after one is deleted, and rebuilt. The id kinds are asserted with
`type::is_int`, since the runner compares record ids across number kinds."""

[[test.results]]
value = "[{ id: pt:[1], point: [1f, 0f] }]"

[[test.results]]
value = "[{ id: pt:[1dec], point: [0f, 1f] }]"

[[test.results]]
value = "[{ id: pt:[2], point: [9f, 9f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = "[{ id: pt:[1], point: [1f, 1f] }]"

[[test.results]]
value = "[{ id: pt:[1dec], point: [2f, 2f] }]"

[[test.results]]
value = "[{ d: 0f, int: true }, { d: 1.4142135623730951f, int: false }]"

[[test.results]]
value = "[{ d: 0f, int: true }, { d: 1.4142135623730951f, int: false }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ int: true }, { int: true }]"

[[test.results]]
value = "[{ int: true }, { int: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ int: true }, { int: true }]"

[[test.results]]
value = "[{ int: true }, { int: true }]"
*/
CREATE pt:[1] SET point = [1f, 0f];
CREATE pt:[1dec] SET point = [0f, 1f];
CREATE pt:[2] SET point = [9f, 9f];
DEFINE INDEX h ON pt FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
DEFINE INDEX d ON pt FIELDS point DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
SELECT type::is_int(id[0]) AS int FROM pt WITH INDEX h WHERE point <|2,100|> [0f, 0f] ORDER BY int;
SELECT type::is_int(id[0]) AS int FROM pt WITH INDEX d WHERE point <|2,100|> [0f, 0f] ORDER BY int;
UPDATE pt:[1] SET point = [1f, 1f];
UPDATE pt:[1dec] SET point = [2f, 2f];
SELECT type::is_int(id[0]) AS int, vector::distance::euclidean(point, [1f, 1f]) AS d FROM pt WITH INDEX h WHERE point <|2,100|> [1f, 1f] ORDER BY d;
SELECT type::is_int(id[0]) AS int, vector::distance::euclidean(point, [1f, 1f]) AS d FROM pt WITH INDEX d WHERE point <|2,100|> [1f, 1f] ORDER BY d;
DELETE pt:[1dec];
SELECT type::is_int(id[0]) AS int FROM pt WITH INDEX h WHERE point <|2,100|> [1f, 1f] ORDER BY int;
SELECT type::is_int(id[0]) AS int FROM pt WITH INDEX d WHERE point <|2,100|> [1f, 1f] ORDER BY int;
REBUILD INDEX h ON pt;
REBUILD INDEX d ON pt;
SELECT type::is_int(id[0]) AS int FROM pt WITH INDEX h WHERE point <|2,100|> [1f, 1f] ORDER BY int;
SELECT type::is_int(id[0]) AS int FROM pt WITH INDEX d WHERE point <|2,100|> [1f, 1f] ORDER BY int;
