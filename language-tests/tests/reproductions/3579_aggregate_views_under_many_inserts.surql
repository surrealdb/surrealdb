/**
[env]
timeout = 5000
namespace = true
database = true

[test]
reason = "Batches of INSERTs into a table with several aggregate views, grouped into array ids, keep every view equal to an aggregate of the source rows. This pins view correctness only. The report's stalls and OOM came from TiKV gRPC keepalive timeouts on a three-node cluster, which 4.0 does not ship, so no language test here can reproduce them."
issue = 3579

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
value = "480"

[[test.results]]
value = "2640"

[[test.results]]
value = "480"

[[test.results]]
value = "2640"

[[test.results]]
value = "2640"

[[test.results]]
value = "2640"

[[test.results]]
value = "12"

[[test.results]]
value = "10"

[[test.results]]
value = "true"
*/

DEFINE TABLE delegation SCHEMALESS;
DEFINE TABLE weekly AS SELECT delegator, collator, year, week, math::sum(amount) AS delegation, count() AS n FROM delegation GROUP BY delegator, collator, year, week;
DEFINE TABLE daily AS SELECT delegator, year, day, math::sum(amount) AS delegation FROM delegation GROUP BY delegator, year, day;
DEFINE TABLE monthly AS SELECT collator, year, month, math::sum(amount) AS delegation, math::max(amount) AS top FROM delegation GROUP BY collator, year, month;

FOR $b IN 0..8 {
	INSERT INTO delegation (array::map(<array> 0..60, |$i| {
		delegator: 'd' + <string> ($i % 13),
		collator: 'c' + <string> ($i % 3),
		year: 2023,
		week: ($b * 60 + $i) % 52,
		month: ($b * 60 + $i) % 12,
		day: ($b * 60 + $i) % 365,
		amount: $i % 10 + 1,
	})) RETURN NONE;
};

count(SELECT id FROM delegation);
math::sum(SELECT VALUE amount FROM delegation);
math::sum(SELECT VALUE n FROM weekly);
math::sum(SELECT VALUE delegation FROM weekly);
math::sum(SELECT VALUE delegation FROM daily);
math::sum(SELECT VALUE delegation FROM monthly);
count(SELECT id FROM monthly);
math::max(SELECT VALUE top FROM monthly);
(SELECT VALUE delegation FROM weekly:['d0', 'c0', 2023, 0]) == (SELECT VALUE math::sum(amount) FROM delegation WHERE delegator = 'd0' AND collator = 'c0' AND week = 0 GROUP ALL);
