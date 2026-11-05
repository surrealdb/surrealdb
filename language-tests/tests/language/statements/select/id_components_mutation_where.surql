/**
[env]
imports = ["./id_components_import.surql"]

[test]
reason = """
	A mutation's WHERE reads the components of the record id as it reads any
	computed field: it derives them for the rows it judges, so an UPDATE or a
	DELETE selects by city and by time without a stored copy of either.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ temp: 4 }, { temp: 5 }, { temp: 16 }, { temp: 17 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ temp: 5 }, { temp: 17 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ temp: 17 }]"
*/

-- UPDATE judges its WHERE on the city.
UPDATE reading SET temp += 10 WHERE city = 'Paris' RETURN NONE;
SELECT temp FROM reading ORDER BY temp;

-- DELETE judges its WHERE on the time.
DELETE reading WHERE time < d'2024-01-02T00:00:00Z' RETURN NONE;
SELECT temp FROM reading ORDER BY temp;

-- Both together name one record.
DELETE reading WHERE city = 'London' AND time = d'2024-01-02T00:00:00Z' RETURN NONE;
SELECT temp FROM reading ORDER BY temp;
