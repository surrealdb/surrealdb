/**
[test]
reason = "Setup for 3.00.00_table_view_aggregate_backfill.surql: the old binary defines aggregating views over rows that already exist, so it fills them itself, and the group columns are named for no name the fill uses."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ amount: 10, id: sale:1, product: 'x', region: 'eu' }]"

[[test.results]]
value = "[{ amount: 20, id: sale:2, product: 'x', region: 'eu' }]"

[[test.results]]
value = "[{ amount: 5, id: sale:3, product: 'y', region: 'eu' }]"

[[test.results]]
value = "[{ amount: 7, id: sale:4, product: 'x', region: 'us' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE TABLE sale SCHEMALESS;
CREATE sale:1 SET region = 'eu', product = 'x', amount = 10;
CREATE sale:2 SET region = 'eu', product = 'x', amount = 20;
CREATE sale:3 SET region = 'eu', product = 'y', amount = 5;
CREATE sale:4 SET region = 'us', product = 'x', amount = 7;
DEFINE TABLE sale_totals AS
    SELECT region, product, count() AS n, math::sum(amount) AS total
    FROM sale
    GROUP BY region, product;
DEFINE TABLE sale_all AS
    SELECT count() AS n, math::sum(amount) AS total
    FROM sale
    GROUP ALL;
