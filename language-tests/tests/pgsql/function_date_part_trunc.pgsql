/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL date_part and date_trunc read a timestamp"

[[test.results]]
value = "[{ dow: 5, doy: 75, q: 1f, y: 2024 }]"

[[test.results]]
value = "[{ h: true, m: true, w: true }]"

*/

SELECT date_part('year', CAST('2024-03-15T10:20:30Z' AS timestamp)) AS y, date_part('dow', CAST('2024-03-15T10:20:30Z' AS timestamp)) AS dow, date_part('doy', CAST('2024-03-15T10:20:30Z' AS timestamp)) AS doy, date_part('quarter', CAST('2024-03-15T10:20:30Z' AS timestamp)) AS q;

SELECT date_trunc('month', CAST('2024-03-15T10:20:30Z' AS timestamp)) = CAST('2024-03-01T00:00:00Z' AS timestamp) AS m, date_trunc('hour', CAST('2024-03-15T10:20:30Z' AS timestamp)) = CAST('2024-03-15T10:00:00Z' AS timestamp) AS h, date_trunc('week', CAST('2024-03-15T10:20:30Z' AS timestamp)) = CAST('2024-03-11T00:00:00Z' AS timestamp) AS w;
