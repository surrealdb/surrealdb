/**
[env]
namespace = true
database = true

[test]
reason = "Aggregates over the columns of a SCHEMAFULL table, grouped by a string, an int or a bool and by nothing, answer what the whole decode of each row holds."

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
value = "[{ count: 40 }]"

[[test.results]]
value = "[{ c: 8, city: 'Cairo' }, { c: 8, city: 'Lima' }, { c: 8, city: 'Oslo' }, { c: 8, city: 'Paris' }, { c: 8, city: 'Tokyo' }]"

[[test.results]]
value = "[{ c: 8, city: 'Cairo', m: 10.25f }, { c: 8, city: 'Lima', m: 9.75f }, { c: 8, city: 'Oslo', m: 10.75f }, { c: 8, city: 'Paris', m: 9.25f }, { c: 8, city: 'Tokyo', m: 8.75f }]"

[[test.results]]
value = "[{ a: 47.50dec, city: 'Cairo', s: 12 }, { a: 46.25dec, city: 'Lima', s: 12 }, { a: 48.75dec, city: 'Oslo', s: 12 }, { a: 45.00dec, city: 'Paris', s: 12 }, { a: 43.75dec, city: 'Tokyo', s: 12 }]"

[[test.results]]
value = "[{ c: 10, m: 9f, tier: 0 }, { c: 10, m: 9.5f, tier: 1 }, { c: 10, m: 10f, tier: 2 }, { c: 10, m: 10.5f, tier: 3 }]"

[[test.results]]
value = "[{ active: false, c: 26, s: 633.75dec }, { active: true, c: 14, s: 341.25dec }]"

[[test.results]]
value = "[{ c: 2, city: 'Cairo', tier: 0 }, { c: 2, city: 'Cairo', tier: 1 }, { c: 2, city: 'Cairo', tier: 2 }, { c: 2, city: 'Cairo', tier: 3 }, { c: 2, city: 'Lima', tier: 0 }, { c: 2, city: 'Lima', tier: 1 }, { c: 2, city: 'Lima', tier: 2 }, { c: 2, city: 'Lima', tier: 3 }, { c: 2, city: 'Oslo', tier: 0 }, { c: 2, city: 'Oslo', tier: 1 }, { c: 2, city: 'Oslo', tier: 2 }, { c: 2, city: 'Oslo', tier: 3 }, { c: 2, city: 'Paris', tier: 0 }, { c: 2, city: 'Paris', tier: 1 }, { c: 2, city: 'Paris', tier: 2 }, { c: 2, city: 'Paris', tier: 3 }, { c: 2, city: 'Tokyo', tier: 0 }, { c: 2, city: 'Tokyo', tier: 1 }, { c: 2, city: 'Tokyo', tier: 2 }, { c: 2, city: 'Tokyo', tier: 3 }]"

[[test.results]]
value = "[{ c: 2, city: 'Cairo' }, { c: 2, city: 'Lima' }, { c: 2, city: 'Oslo' }, { c: 2, city: 'Paris' }, { c: 2, city: 'Tokyo' }]"

[[test.results]]
value = "[{ c: 2, city: 'Cairo' }, { c: 2, city: 'Lima' }, { c: 2, city: 'Oslo' }, { c: 2, city: 'Paris' }, { c: 2, city: 'Tokyo' }]"

[[test.results]]
value = "[{ c: 20, note: NONE }, { c: 1, note: 'n1' }, { c: 1, note: 'n11' }, { c: 1, note: 'n13' }, { c: 1, note: 'n15' }, { c: 1, note: 'n17' }, { c: 1, note: 'n19' }, { c: 1, note: 'n21' }, { c: 1, note: 'n23' }, { c: 1, note: 'n25' }, { c: 1, note: 'n27' }, { c: 1, note: 'n29' }, { c: 1, note: 'n3' }, { c: 1, note: 'n31' }, { c: 1, note: 'n33' }, { c: 1, note: 'n35' }, { c: 1, note: 'n37' }, { c: 1, note: 'n39' }, { c: 1, note: 'n5' }, { c: 1, note: 'n7' }, { c: 1, note: 'n9' }]"

[[test.results]]
value = "[{ count: 8 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 20 }]"

[[test.results]]
value = "[{ hi: 48.75dec, lo: 0dec, m: 9.75f, s: 60 }]"

[[test.results]]
value = "[{ s: 86f }]"

[[test.results]]
value = "[sale:37, sale:38, sale:39]"

[[test.results]]
value = "[sale:4, sale:24]"

[[test.results]]
value = "[{ active: false, amount: 8.75dec, city: 'Lima', id: sale:7, note: 'n7', score: 3.5f, tier: 3 }, { active: true, amount: 33.75dec, city: 'Lima', id: sale:27, note: 'n27', score: 13.5f, tier: 3 }]"

[[test.results]]
value = "[sale:39, sale:38, sale:37, sale:36, sale:35]"

[[test.results]]
value = "[sale:3, sale:8, sale:13, sale:18, sale:23, sale:28, sale:33]"
*/

DEFINE TABLE sale SCHEMAFULL;
DEFINE FIELD city ON sale TYPE string;
DEFINE FIELD tier ON sale TYPE int;
DEFINE FIELD score ON sale TYPE float;
DEFINE FIELD amount ON sale TYPE decimal;
DEFINE FIELD active ON sale TYPE bool;
DEFINE FIELD note ON sale TYPE option<string>;
FOR $i IN 0..40 {
	CREATE type::record('sale', $i) CONTENT {
		city: ['Tokyo', 'Paris', 'Lima', 'Cairo', 'Oslo'][$i % 5],
		tier: $i % 4,
		score: $i * 0.5,
		amount: <decimal> $i * 1.25,
		active: $i % 3 = 0,
		note: IF $i % 2 = 0 { NONE } ELSE { 'n' + <string> $i }
	};
};

SELECT count() FROM sale GROUP ALL;
SELECT city, count() AS c FROM sale GROUP BY city ORDER BY city;
SELECT city, count() AS c, math::mean(score) AS m FROM sale GROUP BY city ORDER BY city;
SELECT city, math::sum(tier) AS s, math::max(amount) AS a FROM sale GROUP BY city ORDER BY city;
SELECT tier, count() AS c, math::mean(score) AS m FROM sale GROUP BY tier ORDER BY tier;
SELECT active, count() AS c, math::sum(amount) AS s FROM sale GROUP BY active ORDER BY active;
SELECT city, tier, count() AS c FROM sale GROUP BY city, tier ORDER BY city, tier;
SELECT city, count() AS c FROM sale WHERE tier = 2 GROUP BY city ORDER BY city;
SELECT city, count() AS c FROM sale WHERE active = true AND score > 5 GROUP BY city ORDER BY city;
SELECT note, count() AS c FROM sale GROUP BY note ORDER BY note;
SELECT count() FROM sale WHERE city = 'Tokyo' GROUP ALL;
SELECT count() FROM sale WHERE tier = 99 GROUP ALL;
SELECT count() FROM sale WHERE note IS NONE GROUP ALL;
SELECT math::sum(tier) AS s, math::mean(score) AS m, math::min(amount) AS lo, math::max(amount) AS hi FROM sale GROUP ALL;
SELECT math::sum(score) AS s FROM sale WHERE city = 'Oslo' GROUP ALL;
SELECT VALUE id FROM sale WHERE score > 18;
SELECT VALUE id FROM sale WHERE city = 'Oslo' AND tier = 0;
SELECT * FROM sale WHERE tier = 3 AND city = 'Lima';
SELECT VALUE id FROM sale ORDER BY score DESC LIMIT 5;
SELECT VALUE id FROM sale ORDER BY city, id LIMIT 7;
