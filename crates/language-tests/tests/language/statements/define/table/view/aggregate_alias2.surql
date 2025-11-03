/**
[test]
issue = 2946

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ average_temp: 27.4f, city: 'London', id: temperatures_by_month:['London', 10], month: 10, total: 1 }"

*/
DEFINE TABLE reading DROP;
DEFINE TABLE temperatures_by_month AS
	SELECT
        city,
		count() AS total,
		time::month(recorded_at) AS month,
		math::mean(temperature) AS average_temp
	FROM reading
	GROUP BY city, month;



CREATE reading SET
	temperature = 27.4,
	recorded_at = d'2025-10-03T14:15:23.12Z',
	city = 'London',
	location = (-0.118092, 51.509865) RETURN NONE;

SELECT * FROM ONLY temperatures_by_month LIMIT 1;
