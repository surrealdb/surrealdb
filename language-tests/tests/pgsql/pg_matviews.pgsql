/**
[env]
namespace = true
database = true

[test]
reason = "pg_matviews is a relation of the catalog, qualified or not, that lists no materialized view even when the database has tables: SurrealDB has no materialized views, and ORMs read the relation when they introspect the schema at start-up"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ amount: 10i32, id: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ relkind: 'v', relname: 'pg_matviews' }]"

[[test.results]]
value = "[]"
*/

CREATE TABLE orders (id INT, amount INT);
INSERT INTO orders (id, amount) VALUES (1, 10);
SELECT * FROM pg_matviews;
SELECT schemaname, matviewname FROM pg_catalog.pg_matviews WHERE schemaname = 'public';
SELECT "schemaname", "matviewname", "matviewowner", "tablespace", "hasindexes", "ispopulated", "definition" FROM "pg_catalog"."pg_matviews";
SELECT count(*) FROM pg_matviews;
SELECT o.id, mv.matviewname FROM orders o INNER JOIN pg_matviews mv ON mv.schemaname = 'public';
SELECT relname, relkind FROM pg_class WHERE relname = 'pg_matviews';
SELECT "t"."name", "v"."definition" FROM (SELECT 'public' AS "schema", 'orders' AS "name") "t" INNER JOIN "pg_matviews" "v" ON "v"."schemaname" = "t"."schema" AND "v"."matviewname" = "t"."name";
