//! The VM's answers to the statements it covers, and the statements it
//! declines.

use std::sync::Arc;

use surrealdb_types::ToSql;

use super::golden::{Golden, render_changes, render_outcome};
use super::plan::{Dependency, Plan};
use crate::exec::operators::test_util::{TestDb, try_collect};
use crate::exec::planner::Planner;
use crate::exec::{ExecOperator, ExecutionContext};
use crate::expr::logical::LogicalOp;
use crate::val::Value;

pub(super) const SCHEMA: &str = "
	DEFINE PARAM $limit VALUE 30;
	DEFINE FUNCTION fn::inc($x: int) { RETURN $x + 1 };
	DEFINE FUNCTION fn::mix($a: int, $b: int) -> int { $a * $b + math::abs($a - $b) };
	DEFINE FUNCTION fn::as_text($x: any) -> string { RETURN $x };
	DEFINE FUNCTION fn::pick($x: int) { RETURN IF $x > 40 THEN [$x, 'old'] ELSE [$x][0] END };
	DEFINE FUNCTION fn::fact($n: int) -> int { IF $n <= 1 { RETURN 1 }; RETURN $n * fn::fact($n - 1) };
	DEFINE FUNCTION fn::named($p: object) -> string { RETURN $p.name ?? 'anon' };
	DEFINE FUNCTION fn::oldest() { RETURN (SELECT VALUE name FROM person ORDER BY age DESC LIMIT 1)[0] };
	CREATE link:1 SET to = person:3, many = [person:1, person:2, 5], geo = (1.0, 2.0), obj = { k: { v: 1 } };
	DEFINE TABLE person SCHEMALESS;
	CREATE person:1 SET name = 'ann', age = 30, tags = ['a', 'b'], nested = { x: 1 };
	CREATE person:2 SET name = 'Bob', age = 17, tags = ['b'];
	CREATE person:3 SET name = 'cat', age = 30, city = 'x';
	CREATE person:4 SET name = 'dan';
	CREATE person:5 SET name = 'Eve', age = 45, nested = { x: 2 };
	FOR $i IN 1..=3000 { CREATE type::record('big', $i) SET n = $i % 7, i = $i };
	DEFINE TABLE mixed SCHEMALESS;
	CREATE mixed:1 SET n = 3;
	CREATE mixed:2 SET n = 3.0f;
	CREATE mixed:3 SET n = '3';
	CREATE mixed:4;
	CREATE mixed:5 SET n = 3dec;
	CREATE mixed:6 SET n = 4;
	CREATE mixed:7 SET n = NULL;
	CREATE mixed:8 SET n = -9223372036854775808;
	DEFINE FIELD n ON mixed TYPE int;
	DEFINE TABLE typed SCHEMAFULL;
	DEFINE FIELD s ON typed TYPE string;
	CREATE typed:1 SET s = 'a';
	DEFINE TABLE indexed SCHEMALESS;
	DEFINE INDEX by_age ON indexed FIELDS age;
	DEFINE INDEX by_name ON indexed FIELDS name UNIQUE;
	DEFINE INDEX by_pair ON indexed FIELDS kind, age;
	FOR $i IN 1..=60 {
		CREATE type::record('indexed', $i) SET age = $i % 9, name = 'n' + <string> $i, kind = $i % 3
	};
	CREATE indexed:tags SET age = [3, 4], name = 'tags';
	CREATE indexed:none SET kind = 1;
	DEFINE TABLE pairs SCHEMALESS;
	DEFINE INDEX by_pair ON pairs FIELDS kind, age;
	FOR $i IN 1..=40 { CREATE type::record('pairs', $i) SET kind = $i % 4, age = ($i * 7) % 11 };
	DEFINE TABLE counted SCHEMAFULL;
	DEFINE FIELD n ON counted TYPE int;
	DEFINE FIELD m ON counted TYPE int;
	DEFINE INDEX all_rows ON counted COUNT;
	DEFINE INDEX small ON counted COUNT WHERE n < 5;
	DEFINE INDEX by_n ON counted FIELDS n;
	DEFINE INDEX by_m ON counted FIELDS m;
	FOR $i IN 1..=30 { CREATE type::record('counted', $i) SET n = $i % 10, m = $i % 3 };
	DEFINE TABLE computed SCHEMAFULL;
	DEFINE FIELD age ON computed TYPE int;
	DEFINE FIELD twice ON computed COMPUTED age * 2;
	CREATE computed:1 SET age = 2;
";

/// Statements the VM covers.
pub(super) const COVERED: &[&str] = &[
	"fn::missing(1)",
	"SELECT * FROM person ORDER BY RAND()",
	// a computed sort key with an effect a projected field shares: a
	// projection function writes its answer to the aliased field
	"SELECT type::field('age') AS a FROM person ORDER BY a",
	// a projection function beside `*`, or under SELECT VALUE
	"SELECT *, type::field('name') FROM person",
	// FETCH, each returned row's record ids replaced by their records.
	"SELECT * FROM link FETCH to",
	"SELECT * FROM link FETCH many",
	"SELECT to, many FROM link FETCH to, many",
	"SELECT * FROM link FETCH nothing",
	"SELECT * FROM person FETCH nested",
	"SELECT * FROM link ORDER BY id LIMIT 1 FETCH to",
	"SELECT VALUE to FROM link FETCH to",
	"SELECT to AS t FROM link FETCH t",
	"SELECT * FROM link FETCH type::field('to')",
	"SELECT * FROM link FETCH many[0]",
	"SELECT * FROM link FETCH many.*",
	"SELECT * FROM link SPLIT many FETCH many",
	"SELECT count() AS n, to FROM link GROUP BY to FETCH to",
	// A computed sort key a projected field shares.
	"SELECT { RETURN age } AS r, name FROM person ORDER BY r DESC",
	"SELECT to.name AS n FROM link ORDER BY n",
	// SPLIT, each row expanded as the rows above it read it.
	"SELECT * FROM person SPLIT tags",
	"SELECT VALUE tags FROM person SPLIT tags",
	"SELECT name AS n, tags AS t FROM person SPLIT t",
	"SELECT tags AS a, tags FROM person SPLIT tags ORDER BY a",
	"SELECT to.name AS x, many FROM link SPLIT many",
	"SELECT *, age AS a OMIT nested FROM person SPLIT tags",
	"SELECT name, tags FROM person SPLIT tags",
	"SELECT * FROM person WHERE age > 18 SPLIT tags",
	"SELECT * FROM person SPLIT tags, nested",
	"SELECT tags FROM person SPLIT tags ORDER BY tags",
	"SELECT tags FROM person SPLIT tags ORDER BY tags DESC LIMIT 2",
	"SELECT * FROM person SPLIT tags LIMIT 2",
	"SELECT * FROM person SPLIT tags LIMIT 0",
	"SELECT * FROM person SPLIT tags START 1 LIMIT 1",
	"SELECT * FROM person SPLIT tags[0]",
	"SELECT * FROM person SPLIT tags[*]",
	"SELECT * FROM big SPLIT n LIMIT 3",
	"SELECT * FROM [{ a: [1, 2] }, { a: 3 }] SPLIT a",
	"SELECT * FROM person:1 SPLIT tags",
	"SELECT * FROM person, link SPLIT many",
	"SELECT * FROM link SPLIT many",
	"SELECT * FROM person",
	"SELECT * FROM person WHERE age > 18",
	"SELECT * FROM person WHERE age > 18 AND name = 'cat'",
	"SELECT * FROM person WHERE age > 18 OR !city",
	"SELECT * FROM person WHERE NOT (age > 18 AND city = 'x')",
	"SELECT * FROM person WHERE age IN [17, 45]",
	"SELECT * FROM person WHERE tags CONTAINS 'b'",
	"SELECT * FROM person WHERE string::len(name) > 2",
	"SELECT * FROM person WHERE nested.x = 2",
	"SELECT * FROM person WHERE age",
	"SELECT name, age AS a, age + 1 AS b FROM person WHERE age >= 1 LIMIT 3 START 1",
	"SELECT name, nested.x FROM person",
	"SELECT nested.x AS nx, missing FROM person",
	"SELECT VALUE name FROM person",
	"SELECT VALUE age > 18 FROM person",
	"SELECT * FROM person ORDER BY age DESC, name",
	"SELECT * FROM person ORDER BY name COLLATE LIMIT 2 START 1",
	"SELECT * FROM person ORDER BY name NUMERIC",
	"SELECT * FROM person ORDER BY age LIMIT 0",
	"SELECT * FROM person LIMIT 0",
	"SELECT * FROM person START 10",
	"SELECT * FROM person LIMIT 2 + 1",
	"SELECT * FROM big WHERE n = 3 ORDER BY i DESC LIMIT 5",
	"SELECT VALUE i FROM big WHERE n > 2",
	"SELECT * FROM big WHERE n = 3 LIMIT 700 START 20",
	"SELECT * FROM big ORDER BY n, i DESC LIMIT 100 START 950",
	"SELECT * FROM indexed",
	"SELECT * FROM person LIMIT 'x'",
	"SELECT * FROM person LIMIT $limit - 28 START [1, 0][$]",
	"SELECT * FROM person LIMIT [2, 3][$this ?? 0] START [1, 0][$this ?? 0]",
	"SELECT * FROM person LIMIT (SELECT VALUE age FROM person ORDER BY age LIMIT 1)[0] - 17",
	"SELECT name FROM person ORDER BY age LIMIT [2, 3][$this ?? 0] TIMEOUT [5s, 6s][$this ?? 0]",
	"SELECT * FROM person LIMIT <int> 'x'",
	"SELECT * FROM type::record('person', $limit - 29)",
	// a projected `RETURN` is the field's value, at the field's root only
	"SELECT VALUE { RETURN age } FROM person",
	"SELECT { RETURN name } AS n, age FROM person",
	"SELECT ({ RETURN 1 }) = 1 AS a FROM person",
	"SELECT * FROM person WHERE <int> name = 1",
	// `*` beside other fields, and OMIT of top-level fields
	"SELECT *, name AS n FROM person",
	"SELECT * OMIT name FROM person",
	"SELECT * OMIT name, tags FROM person WHERE age",
	"SELECT *, age + 1 AS age FROM person",
	"SELECT *, nested.x AS nested FROM person ORDER BY age",
	"SELECT *, { RETURN 1 } AS r OMIT id FROM person",
	// the tail: TIMEOUT, VERSION and ONLY, applied by the program
	"SELECT * FROM person TIMEOUT 1s",
	"SELECT name FROM person WHERE age > 18 LIMIT 1 TIMEOUT 5s",
	"SELECT * FROM ONLY person LIMIT 1",
	"SELECT * FROM ONLY person WHERE age > 40",
	"SELECT * FROM ONLY person",
	"SELECT * FROM ONLY person WHERE age > 1000",
	"SELECT VALUE name FROM ONLY person WHERE age = 45 LIMIT 1",
	"SELECT count() FROM person GROUP ALL TIMEOUT 5s",
	// record-id sources: a point read, and a range in key order
	"SELECT * FROM person:1",
	"SELECT * FROM person:99",
	"SELECT * FROM ONLY person:1",
	"SELECT * FROM ONLY person:99",
	"SELECT name FROM person:1..4",
	"SELECT * FROM person:2.. WHERE age > 18",
	"SELECT * FROM person:1..=3 LIMIT 1 START 1",
	"SELECT * FROM big:10..40 WHERE n = 3",
	"SELECT count() AS c, math::sum(i) AS s FROM big:1..100 GROUP ALL",
	"SELECT * FROM big:1..=5 ORDER BY i DESC",
	"SELECT * FROM indexed:1",
	"SELECT * FROM person:1 WHERE age > 40",
	// `FROM a, b`: each source in written order, paged across them
	"SELECT * FROM person, big WHERE n = 3",
	"SELECT * FROM person:1, person:2",
	"SELECT * FROM person:3, person:1, person:99",
	"SELECT * FROM big:1..3, person:1..3",
	"SELECT name FROM person, typed LIMIT 3",
	"SELECT * FROM person, big LIMIT 7",
	"SELECT * FROM person, mixed WHERE n = 3 LIMIT 2 START 1",
	"SELECT * FROM person, big ORDER BY age, i LIMIT 4",
	"SELECT count() FROM person, big GROUP ALL",
	"SELECT * FROM person, indexed",
	// values, arrays, parameters, mocks and subqueries in FROM, whose rows the
	// VM filters, pages and projects
	"SELECT * FROM [1, 2, 3] WHERE $this > 1",
	"SELECT * FROM [person:1, person:2]",
	"SELECT * FROM [person:1, person:99, 5]",
	"SELECT * FROM $nothing",
	"SELECT * FROM |person:1..3|",
	"SELECT name FROM (SELECT * FROM person WHERE age > 18) LIMIT 1",
	"SELECT * FROM (SELECT VALUE name FROM person)",
	"SELECT * FROM array::range(1, 3)",
	"SELECT * FROM { a: 1 }",
	"SELECT a FROM [{ a: 1 }, { a: 2 }, 3] WHERE a > 1",
	"SELECT * FROM [1, 2, 3, 4] LIMIT 2 START 1",
	"SELECT * FROM [{ bad: 'x' }] WHERE <int> bad = 1 LIMIT 0",
	"SELECT * FROM [{ bad: 1 }, { bad: 'x' }] WHERE <int> bad = 1 LIMIT 0",
	"SELECT type::field('a') FROM [{ a: 1 }]",
	"SELECT VALUE $this * 2 FROM [1, 2, 3]",
	// projection functions, written as the projection operator writes them
	"SELECT type::field('name') FROM person",
	"SELECT type::field('name') AS n, age FROM person",
	"SELECT type::fields(['name', 'age']) FROM person WHERE age",
	"SELECT type::fields(['name', 'age']) AS f FROM person",
	"SELECT VALUE type::field('name') FROM person",
	"SELECT VALUE type::fields(['name', 'age']) FROM person",
	"SELECT *, type::field('name') AS again FROM person",
	// a set literal, built as its elements are evaluated
	"SELECT VALUE {age, 1, age} FROM person",
	// index operators with no clause in scope answer false, reading nothing
	"'abc' @@ 'zzz'",
	"[1.0, 2.0] <|1, EUCLIDEAN|> [1.0, 2.0]",
	// ORDER BY under a projection: keys read from the source row, resolved
	// against the projection first
	"SELECT VALUE name FROM person ORDER BY age",
	"SELECT name, age FROM person ORDER BY age DESC, name",
	"SELECT name FROM person ORDER BY age",
	"SELECT age AS a, name FROM person ORDER BY a",
	"SELECT name AS age FROM person ORDER BY age",
	"SELECT VALUE age FROM person ORDER BY age LIMIT 2 START 1",
	"SELECT name, age + 1 AS b FROM person WHERE age ORDER BY age LIMIT 3",
	"SELECT i FROM big WHERE n = 3 ORDER BY i DESC LIMIT 5",
	// sort keys that are paths, and keys computed before the sort
	"SELECT * FROM person ORDER BY nested.x",
	"SELECT name FROM person ORDER BY nested.x DESC, name",
	"SELECT age + 1 AS a FROM person ORDER BY a",
	"SELECT age * 2 AS d, name FROM person ORDER BY d DESC LIMIT 2",
	"SELECT string::len(name) AS l, name FROM person ORDER BY l, name",
	"SELECT VALUE name FROM person ORDER BY missing, name",
	"SELECT i % 5 AS m, i FROM big WHERE n = 3 ORDER BY m, i LIMIT 7",
	// GROUP ALL, folded in the scan
	"SELECT count() FROM person GROUP ALL",
	"SELECT name, count() FROM person GROUP ALL",
	"SELECT count() AS c FROM person WHERE age > 18 GROUP ALL",
	"SELECT count(age) AS c, math::sum(age) AS s, math::min(age) AS lo, math::max(age) AS hi, math::mean(age) AS m FROM person GROUP ALL",
	"SELECT count() FROM person WHERE age > 1000 GROUP ALL",
	"SELECT math::sum(age) AS s FROM person WHERE age > 1000 GROUP ALL",
	"SELECT VALUE count() FROM person GROUP ALL",
	"SELECT count() * 2 AS twice, 'k' AS label FROM person GROUP ALL",
	"SELECT array::group(tags) AS g FROM person GROUP ALL",
	"SELECT array::join(name, ', ') AS names FROM person GROUP ALL",
	"SELECT math::sum(n) AS t FROM mixed GROUP ALL",
	"SELECT math::sum(n) AS t FROM mixed WHERE n = 'x' GROUP ALL",
	"SELECT math::min(n) AS lo, math::max(n) AS hi FROM mixed GROUP ALL",
	"SELECT math::sum(i) AS s, count() AS c FROM big WHERE n = 3 GROUP ALL",
	// GROUP BY, folded in the scan and reported in group-key order
	"SELECT city, count() FROM person GROUP BY city",
	"SELECT age, count() AS c, array::group(name) AS names FROM person GROUP BY age",
	"SELECT city, age, count() FROM person GROUP BY city, age",
	"SELECT age AS a, count() FROM person GROUP BY a",
	"SELECT age, count() FROM person WHERE age > 1000 GROUP BY age",
	"SELECT VALUE city FROM person GROUP BY city",
	"SELECT city, math::sum(age) AS s, math::max(age) AS hi FROM person WHERE age GROUP BY city",
	"SELECT n, count() FROM mixed GROUP BY n",
	"SELECT n, count() AS c, math::sum(i) AS s FROM big GROUP BY n",
	"SELECT name, count() FROM person GROUP BY name",
	// a field declared `int` holding values stored before the declaration
	"SELECT * FROM mixed WHERE n = 3",
	"SELECT * FROM mixed WHERE n == 3",
	"SELECT * FROM mixed WHERE n != 3",
	"SELECT * FROM mixed WHERE n > 2",
	"SELECT * FROM mixed WHERE n <= 3",
	"SELECT * FROM mixed WHERE 3 < n",
	"SELECT * FROM mixed WHERE 3 >= n",
	"SELECT * FROM mixed WHERE n < -9223372036854775807",
	"SELECT VALUE id FROM mixed WHERE n = 3 OR n = 4",
	"SELECT * FROM typed WHERE s = 3",
	"SELECT * FROM person WHERE id = person:1",
	"SELECT * FROM person WHERE id > person:2 AND age != NONE",
	"SELECT * FROM person WHERE age = 30.0f OR age = 45dec",
	"SELECT * FROM person WHERE name > 'b' AND name < 'd'",
	"SELECT * FROM person WHERE nested = { x: 1 }",
	"SELECT * FROM person WHERE tags *= 'b' OR tags ?= 'a'",
	"SELECT VALUE nested FROM person WHERE city",
	"SELECT * FROM big WHERE n = 3 AND i > 2990",
	"SELECT * FROM computed",
	"SELECT twice FROM computed WHERE twice > 3",
	"SELECT * FROM indexed WHERE age = 3",
	"SELECT * FROM indexed WHERE age > 6",
	"SELECT * FROM indexed WHERE age >= 2 AND age < 4 AND kind = 1",
	"SELECT id FROM indexed WHERE name = 'n7'",
	"SELECT * FROM indexed WHERE name > 'n5'",
	"SELECT * FROM indexed WHERE kind = 2 AND age > 4",
	"SELECT id, age FROM indexed ORDER BY age",
	"SELECT kind AS age, id FROM indexed ORDER BY age, id",
	"SELECT age AS age, id FROM indexed ORDER BY age DESC",
	"SELECT id, age FROM indexed ORDER BY age DESC LIMIT 7",
	"SELECT id, age FROM indexed WHERE age > 2 ORDER BY age DESC LIMIT 5 START 2",
	"SELECT id FROM indexed WHERE kind = 1 ORDER BY age DESC",
	"SELECT id FROM indexed WHERE age = 4 LIMIT 3",
	"SELECT * FROM indexed WHERE age = 3 AND name != 'n3'",
	"SELECT VALUE name FROM indexed WHERE age = NONE",
	"SELECT count() FROM indexed WHERE age = 3 GROUP ALL",
	"SELECT math::sum(age) AS s FROM indexed WHERE age > 5 GROUP ALL",
	"SELECT kind, count() FROM indexed WHERE age > 5 GROUP BY kind",
	"SELECT * FROM indexed WITH NOINDEX WHERE age = 3",
	"SELECT * FROM indexed WITH INDEX by_pair WHERE kind = 1 AND age = 4",
	"SELECT * FROM indexed WHERE id = indexed:5 AND age = 5",
	"SELECT * FROM person, indexed WHERE age = 3",
	"SELECT * FROM indexed WHERE age = 3 OR age = 4",
	"SELECT * FROM indexed WHERE age IN [3, 4]",
	"SELECT id FROM indexed WHERE age IN [3, 4] AND name != 'n12'",
	"SELECT id FROM indexed WHERE age = 3 OR age = 5 ORDER BY id DESC",
	"SELECT id FROM indexed WHERE age = 3 OR age = 5 ORDER BY id LIMIT 4",
	"SELECT id, age FROM indexed WHERE kind IN [1, 2] ORDER BY age",
	"SELECT id, age FROM indexed WHERE kind IN [1, 2] ORDER BY age DESC LIMIT 5",
	"SELECT id FROM indexed WHERE age CONTAINSANY [3, 4]",
	"SELECT id, age FROM indexed WHERE age CONTAINSANY [3, 4] ORDER BY age",
	"SELECT count() FROM indexed WHERE age = 3 OR age = 4 GROUP ALL",
	"SELECT * FROM indexed WHERE kind = 2 AND age = 5",
	"SELECT id FROM indexed WHERE age = 3 OR name = 'n4' OR age = 5",
	"SELECT id FROM indexed WHERE age > 2 AND kind = 1 AND name != 'n4'",
	"SELECT id FROM indexed WHERE age = 3 AND !(kind = 0)",
	"SELECT id, name FROM indexed WHERE age = 4 OR name = 'n9' ORDER BY name LIMIT 3",
	"SELECT count() FROM indexed WHERE kind = 2 AND age = 5 GROUP ALL",
	"SELECT id, kind, age FROM pairs WHERE kind IN [1, 2] ORDER BY age",
	"SELECT count() FROM person GROUP ALL LIMIT 1",
	"SELECT count() FROM person GROUP ALL LIMIT 1 START 1",
	"SELECT VALUE count() FROM person GROUP ALL",
	"SELECT count() AS c, count() AS d FROM big GROUP ALL",
	"SELECT count() FROM big:10..20 GROUP ALL",
	"SELECT count() FROM person:1 GROUP ALL",
	"SELECT count() FROM counted GROUP ALL",
	"SELECT count() FROM counted WHERE n < 5 GROUP ALL",
	"SELECT count() FROM counted WHERE n = 3 GROUP ALL",
	"SELECT count() FROM counted WHERE n > 6 GROUP ALL",
	"SELECT count() FROM counted WHERE n = 3 AND m = 1 GROUP ALL",
	"SELECT count() FROM counted WITH NOINDEX WHERE n = 3 GROUP ALL",
	"SELECT count() FROM indexed WHERE name != 'x' GROUP ALL",
	"SELECT n, count() AS c FROM big GROUP BY n ORDER BY c DESC LIMIT 2",
	"SELECT n, count() AS c FROM big GROUP BY n ORDER BY n DESC",
	"SELECT n, count() AS c FROM big GROUP BY n LIMIT 3 START 1",
	"SELECT n, count() AS c OMIT c FROM big GROUP BY n",
	"SELECT n, math::sum(i) AS s FROM big WHERE i > 100 GROUP BY n ORDER BY s LIMIT 3",
	"SELECT city, count() AS c FROM person GROUP BY city ORDER BY c",
	"SELECT n, count() AS c FROM big GROUP BY n ORDER BY c DESC, n DESC LIMIT 4",
	"SELECT count() AS c FROM big GROUP ALL LIMIT 0",
	"SELECT city, 'x' AS label, count() FROM person GROUP BY city",
	"SELECT count(age > 18) AS c FROM person GROUP ALL",
	"SELECT math::sum(i * 2) AS s, count(n = 3) AS t FROM big GROUP ALL",
	"SELECT n, math::max(i % 100) AS m FROM big GROUP BY n",
	"SELECT city, name FROM person GROUP BY city",
	"SELECT n, i FROM big GROUP BY n",
	"SELECT name, count() AS c FROM person GROUP ALL",
	"SELECT count(<int> name) AS c, math::sum(<int> age) AS s FROM person GROUP ALL",
	"SELECT count() AS c FROM [1, 2, 3, 'x'] GROUP ALL",
	"SELECT math::sum($value) AS s FROM [1, 2, 3] GROUP ALL",
	"SELECT v, count() AS c FROM [{ v: 1 }, { v: 2 }, { v: 1 }] GROUP BY v",
	"SELECT count() AS c FROM (SELECT * FROM person WHERE age > 18) GROUP ALL",
	"SELECT age, count() AS c FROM (SELECT age FROM person) GROUP BY age ORDER BY c DESC",
	"SELECT id, kind, age FROM pairs WHERE kind IN [1, 3] ORDER BY age DESC LIMIT 6",
	"SELECT id FROM pairs WHERE kind = 1 OR kind = 2 ORDER BY age LIMIT 3 START 2",
	// expression statements
	"1 + 1",
	"[1, 2, 3]",
	"IF true { 1 } ELSE { 2 }",
	"math::sum([1, 2])",
	"(SELECT name FROM person WHERE age > 18)",
	"person:1.name",
	"7 % 0",
	// Expressions compiled to instructions: the operand semantics of
	// `operand_semantics.surql`.
	"SELECT age + 1 AS next, age * 2 - 1 AS odd, age / 7 AS div, -age AS neg FROM person",
	"SELECT age AND name AS both, age OR name AS either, age ?? 'none' AS aged, age ?: 0 AS any FROM person",
	"SELECT [age, name, 1] AS row, { a: age, b: [name] } AS obj FROM person",
	"SELECT <string> age AS text, !age AS missing, age..45 AS span, ..=age AS upto FROM person",
	"SELECT * FROM person WHERE age + 1 > 31",
	"SELECT * FROM person WHERE (age ?? 0) < 20",
	"SELECT * FROM person WHERE age > $limit",
	"SELECT age > $limit AS over, $limit AS limit FROM person",
	"SELECT n + 1 AS next FROM mixed",
	"SELECT n * 2 AS twice FROM mixed WHERE n != NONE",
	"SELECT n - 1 AS prev FROM mixed WHERE n = -9223372036854775808",
	"SELECT age % 0 AS rem FROM person WHERE age = 30",
	"SELECT age / 0 AS div FROM person",
	"SELECT 1 + 1.5 AS f, 1 + 1dec AS d, 7 / 2 AS t, 2 ** 10 AS p FROM person:1",
	"SELECT (false AND <int> 'x') AS lazy, (NULL AND 1) AS n, ('' OR 'b') AS o FROM person:1",
	"SELECT <int> name AS bad FROM person",
	"SELECT age > 18 AND name = 'cat' AS flag FROM person",
	"SELECT age.. AS from, age>..50 AS skip FROM person",
	// Builtins called by name, their arguments compiled.
	"SELECT string::len(name) AS l, math::max([age, 1]) AS m, type::is_string(name) AS s FROM person",
	"SELECT * FROM person WHERE string::lowercase(name) = 'bob'",
	"SELECT math::sqrt(name) AS bad FROM person",
	"SELECT array::len(tags ?? []) + 1 AS n, string::concat(name, '-', <string> age) AS tag FROM person",
	"SELECT type::table('person') AS t, string::uppercase($limit ?? 'x') AS p FROM person:1",
	"string::len('abc') * 2",
	// Path steps: an object's field, a record link fetched, an array read
	// element by element, a geometry's GeoJSON field, NONE past a gap.
	"SELECT obj.k.v AS v, to.name AS n, many.name AS ns, geo.type AS gt, to.missing.deep AS none FROM link",
	"SELECT * FROM link WHERE to.age = 30 AND obj.k.v = 1",
	"SELECT nested.x + 1 AS x, (nested ?? {}).x AS y FROM person",
	"SELECT tags[0] AS first, tags[$] AS last, tags[1] AS second, tags[age % 2] AS picked FROM person",
	"SELECT many[0].name AS first, [[1, 2], [3]]… AS flat, obj['k'] AS keyed, 5[0] AS scalar FROM link",
	"SELECT tags['x'] AS bad FROM person",
	// Methods: dispatched on the receiver's type, their arguments read from
	// the record, and the unknown-method error for a type with no builtin.
	"SELECT name.len() AS l, tags.len() AS t, name.uppercase() AS u, obj.keys() AS k FROM person",
	"SELECT tags.slice(0, age % 2) AS s, name.to_string() AS n FROM person",
	"SELECT * FROM person WHERE name.len() > 3",
	"SELECT age.uppercase() AS u FROM person",
	"SELECT tags.slice('x') AS bad FROM person",
	"'abc'.len() + $limit",
	"[1, 2][age] ?? 'abc'.slice(age)",
	// An argument or an index read inside a mapped step is read against the
	// row a projection's field is evaluated over, and against the value
	// stepped from where the statement runs over no row.
	"SELECT VALUE [stats.items.slice(0, count), stats.items[count], stats.items[$this.count]] \
	 FROM [{ stats: { items: [1, 2, 3], count: 9 }, count: 2 }, \
	 { stats: [{ items: [4, 5], count: 0 }, { items: [6], count: 0 }], count: 1 }]",
	"[{ a: [1, 2], i: 1 }, { a: [3], i: 0 }].a[i]",
	// A mapped `.*` fetches an element that is a record id and passes any
	// other element through.
	"[[3, 2, 1], [1, 2, 3]].*[0..1].min()",
	"[person:1, person:2, 5, { a: 1 }, [person:3]].*.name",
	"SELECT VALUE many.*.name FROM link",
	// A mapped step's closure argument that reads only its parameters.
	"[{ age: 30 }, { age: 31 }].age.expect(|$a| $a > 5)",
	"[{ age: 30 }, { age: 3 }].age.expect(|$a| $a > 5)",
	"SELECT VALUE [nested].x.map(|$x| $x * 2) FROM person",
	"[{ a: 'x-y', sep: '-' }].a.split(sep)",
	"SELECT VALUE (SELECT VALUE 1 FROM $this.text.split($this.sep)) \
	 FROM [{ text: 'a-b', sep: '-' }, { text: 'c/d/e', sep: '/' }]",
	"SELECT VALUE (SELECT VALUE $this FROM $this.text[$this.sep]) \
	 FROM [{ text: { '-': [7, 8] }, sep: '-' }]",
	// Record ids with computed keys.
	"SELECT person:[name, age + 1] AS a, person:{ n: name, t: tags[0] } AS o FROM person",
	"SELECT * FROM person WHERE id = type::record('person', age % 3 + 1) OR person:[age] = person:[45]",
	"person:[$limit, 'x']",
	"person:[$limit]..=[$limit + 1]",
	"person:..[$limit]",
	"person:{ a: $limit, b: NONE }",
	"<string> person:[1 / 0]",
	"person:[<int> 'x']",
	// A `BREAK` or `CONTINUE` in a key element has no loop to take it.
	"person:[BREAK]",
	"person:{ a: 1, b: CONTINUE }",
	"FOR $i IN [1, 2] { person:[$i, BREAK] }",
	"person:[IF true { BREAK } ELSE { 1 }]",
	"FOR $i IN [1, 2] { person:[IF $i > 1 { CONTINUE } ELSE { $i }] }",
	"SELECT person:[({ RETURN age }), name] AS k, person:{ a: ({ RETURN 1 }) + 1 } AS o FROM person",
	// IF as a value: the first truthy condition's value, the ELSE, or NONE.
	"SELECT IF age > 40 THEN 'old' ELSE IF age > 20 THEN name.uppercase() END AS a FROM person",
	"SELECT IF tags THEN tags[0] ELSE 'none' END AS t FROM person WHERE IF age > 20 { true } ELSE { false }",
	"IF $limit > 3 THEN 1 ELSE <int> 'x' END",
	"IF $limit < 3 THEN 1 ELSE <int> 'x' END",
	"IF $limit < 3 THEN 1 END",
	// Subqueries: planned with the statement, run over the row.
	"SELECT name, (SELECT VALUE name FROM person WHERE age > $parent.age ORDER BY name) AS older FROM person",
	"SELECT (SELECT * FROM ONLY person:1).name AS n, (SELECT VALUE age FROM person:2)[0] + 1 AS a FROM person",
	"SELECT * FROM person WHERE (SELECT VALUE count() FROM person WHERE age < $parent.age GROUP ALL)[0] > 0",
	"SELECT ({ RETURN age }) + 1 AS a, IF age > 20 THEN ({ RETURN 'r' }) ELSE 0 END AS b FROM person",
	"SELECT ({ age + 1 }) AS a, ($this.name) AS n FROM person",
	"(SELECT VALUE name FROM person ORDER BY name)[0]",
	"(SELECT VALUE name FROM person WHERE <int> 'x')",
	"({ RETURN 5 }) * 2",
	// User-defined functions: entered, their arguments evaluated in the
	// frame the entry opens, and their bodies run as subroutines.
	"SELECT fn::inc(age) AS a, fn::named($this) AS n FROM person",
	"SELECT * FROM person WHERE fn::inc(age) > 20 ORDER BY age",
	"fn::fact(6) + fn::inc($limit)",
	"fn::oldest()",
	"fn::inc('x')",
	"fn::inc(1, 2)",
	"fn::inc(({ RETURN 3 }))",
	"SELECT fn::inc(({ RETURN age })) AS a FROM person",
	// Inlined bodies: the arguments bound and coerced, the return coerced.
	"SELECT fn::mix(age, 3) AS m, fn::pick(age) AS p, fn::as_text(name) AS t FROM person",
	"fn::mix(2)",
	"fn::mix('a', 1)",
	"fn::as_text(1)",
	"fn::inc(fn::mix(2, 3))",
	// Closure builtins run as loops in place over a closure written in the
	// call: what each passes, where it stops, and the errors on the way.
	"SELECT array::map(tags, |$t| string::uppercase($t)) AS u, array::map([1, 2, 3], |$v, $i| $v * $i) AS m FROM person",
	"array::filter([1, 2, 3, 4], |$v| $v % 2 = 0)",
	"array::filter_index([1, 2, 3, 4], |$v| $v > 2)",
	"[array::find([1, 2, 3], |$v| $v > 1), array::find([1], |$v| $v > 1)]",
	"[array::find_index([1, 2, 3], |$v| $v > 1), array::index_of([1], |$v| $v > 5)]",
	"[array::any([1, 2], |$v| $v > 1), array::some([1], |$v| $v > 1), array::includes([], |$v| $v)]",
	"[array::all([1, 2], |$v| $v > 0), array::every([1, 0], |$v| $v > 0), array::all([], |$v| $v)]",
	"array::any([1, 'a'], |$v: int| $v > 0)",
	"array::all([0, 'a'], |$v: int| $v > 0)",
	"array::map([1, 'a'], |$v: int| $v)",
	"array::map('x', |$v| $v)",
	"array::map([1, 2], |$v| -> string { $v })",
	"array::map([1, 2], |$v| -> int { $v + 1 })",
	"array::map([], |$v| $v)",
	"array::map([1, 2], || 5)",
	"array::map([1, 2], |$v: int| -> string { RETURN $v * 2 })",
	"SELECT array::map(tags, |$t| name) AS n FROM person",
	"SELECT * OMIT nested.x FROM person",
	"SELECT name OMIT age FROM person",
	// An OMIT destructure removes the fields it lists, not the object
	// holding them.
	"SELECT * OMIT nested.{ x } FROM person",
	"SELECT * OMIT obj.k.{ v } FROM link",
	"SELECT * OMIT obj.{ k.{ v } } FROM link",
	"SELECT * OMIT obj.{ k }.v FROM link",
	"SELECT obj OMIT obj.k.{ v } FROM link",
	// An OMIT path known only once walked: a lookup that names no location, a
	// destructure entry that empties a field, one that names an expression
	// and a rest entry, a filter, an index read from a parameter, `.*`, a
	// method call and a projection function.
	"SELECT * OMIT ->knows FROM person",
	"SELECT * OMIT obj.{ k.* } FROM link",
	"SELECT * OMIT obj.{ a: k } FROM link",
	"SELECT * OMIT obj.{ k, ... } FROM link",
	"SELECT * OMIT tags[WHERE $this > 1] FROM person",
	"SELECT * OMIT tags[WHERE true] FROM person",
	"{ LET $i = 0; SELECT * OMIT tags[$i] FROM person }",
	"SELECT * OMIT tags[0], tags[$] FROM person",
	"SELECT * OMIT nested.* FROM person",
	"SELECT * OMIT tags.len() FROM person",
	"SELECT * OMIT type::field('age'), type::fields(['name', 'tags']) FROM person",
	"SELECT name, age OMIT age FROM person",
	"SELECT * OMIT tags[WHERE $this > 1] FROM [{ tags: [1, 2, 3] }]",
	"SELECT * OMIT a.{ b }.x FROM [{ a: { b: { x: 1, y: 2 } } }]",
	// OMIT under SELECT VALUE is removed from the row the value reads.
	"SELECT VALUE name OMIT name FROM person",
	"SELECT VALUE age OMIT name FROM person ORDER BY name DESC",
	"SELECT VALUE nested OMIT nested.x FROM person",
	"SELECT VALUE tags OMIT tags[WHERE $this > 1] FROM person",
	"SELECT VALUE id OMIT name FROM person:1",
	"SELECT VALUE a OMIT a.b FROM [{ a: { b: 1, c: 2 } }]",
	"SELECT VALUE name OMIT name FROM [1, { name: 'x' }]",
	"SELECT * FROM person EXPLAIN",
	"SELECT * FROM missing",
	"SELECT * FROM person WHERE (CREATE log:1)",
];

/// Statements the VM declines, with what makes it decline them.
const DECLINED: &[&str] = &[];

/// The state each write statement runs against, from scratch every time.
pub(super) const WRITE_SCHEMA: &str = "
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE log SCHEMALESS;
	DEFINE TABLE m SCHEMALESS;
	DEFINE TABLE knows SCHEMALESS;
	CREATE person:1 SET name = 'ann', age = 30;
	CREATE person:2 SET name = 'Bob', age = 17;
	CREATE person:3 SET name = 'cat', age = 45;
	DEFINE TABLE uniq SCHEMALESS;
	DEFINE INDEX by_email ON uniq FIELDS email UNIQUE;
	CREATE uniq:1 SET email = 'a@x', n = 1;
	DEFINE TABLE strict SCHEMAFULL;
	DEFINE FIELD req ON strict TYPE string;
	DEFINE FIELD n ON strict TYPE int DEFAULT 0;
	CREATE strict:1 SET req = 'r';
	DEFINE TABLE watched SCHEMALESS;
	DEFINE EVENT noted ON watched WHEN $event = 'CREATE' THEN (CREATE log SET of = $after.id);
";

/// The tables whose records a write statement's effect is judged by.
const WRITE_TABLES: &[&str] = &["person", "uniq", "strict", "watched", "log", "m", "knows"];

/// Write statements the VM covers, each run on a fresh database and judged by
/// its rows and by the records it leaves.
pub(super) const WRITES: &[&str] = &[
	"CREATE person:10 SET name = 'x'",
	"CREATE person SET name = 'y'",
	"CREATE person:1 SET name = 'dup'",
	"CREATE ONLY person:11 SET a = 1",
	"CREATE ONLY person:11, person:12 SET a = 1",
	"CREATE person:12, person:13 SET a = 1 RETURN NONE",
	"CREATE person:14 CONTENT { a: 1 } RETURN a",
	"CREATE |m:1..3| SET a = 1",
	"CREATE person:1..3",
	"CREATE watched:1 SET v = 1",
	"CREATE strict:2 SET n = 1",
	"UPDATE person SET seen = true",
	"UPDATE person SET age += 1 WHERE age > 20 RETURN AFTER",
	"UPDATE person:1 SET name = 'z' RETURN BEFORE",
	"UPDATE ONLY person:1 SET n = 1",
	"UPDATE ONLY person SET n = 1",
	"UPDATE person:99 SET n = 1",
	"UPDATE person:1..3 SET r = true",
	"DELETE person WHERE age < 20",
	"DELETE person:1 RETURN BEFORE",
	"DELETE ONLY person:2 RETURN BEFORE",
	"DELETE person:99",
	"UPSERT person:1 SET name = 'u'",
	"UPSERT person:50 SET name = 'new'",
	"UPSERT person:1 CONTENT { name: 'c' }",
	"UPSERT person SET flag = true WHERE age > 100",
	"UPSERT ONLY person SET flag = true WHERE age > 100",
	"UPSERT person SET flag = true WHERE age > 20",
	"UPSERT uniq SET email = 'a@x', n += 1",
	"UPSERT uniq:2 SET email = 'a@x'",
	"UPSERT strict:1 SET n = 5",
	"UPSERT strict:2 SET n = 5",
	"UPSERT strict:2 SET req = 'q', n = 5",
	"INSERT INTO person { id: 20, name: 'i' }",
	"INSERT INTO person [{ id: 21 }, { id: 22 }]",
	"INSERT INTO person (id, name) VALUES (23, 'a'), (24, 'b')",
	"INSERT INTO person { id: 1, name: 'dup' }",
	"INSERT IGNORE INTO person { id: 1, name: 'dup' }",
	"INSERT INTO person { id: 1, name: 'dup' } ON DUPLICATE KEY UPDATE name = 'kept'",
	"INSERT INTO uniq { email: 'a@x' } ON DUPLICATE KEY UPDATE n += 10",
	"INSERT RELATION INTO knows { in: person:1, out: person:2 }",
	"RELATE person:1->knows->person:2",
	"RELATE person:1->knows->[person:2, person:3] SET w = 1",
	"RELATE [person:1, person:2]->knows:k->person:3",
	"RELATE ONLY person:1->knows->[person:2, person:3]",
	"RELATE person:1->knows->(1 + 1)",
	"(CREATE person:30 SET v = 1).v",
	// targets and endpoints the body computes
	"CREATE type::record('person', $limit + 1) SET a = 1",
	"CREATE [person:40, type::record('person', 41)][$] SET a = 1",
	"CREATE $limit SET a = 1",
	"UPDATE type::table('person') SET t = 1 WHERE age > 20",
	"RELATE (type::record('person', 1))->knows->([person:2, person:3][0])",
	"RELATE person:1->knows->$limit",
];

/// Generated ids differ between runs, so a record id whose key is a
/// generated string reads as one placeholder.
pub(super) fn without_generated_ids(value: Value) -> Value {
	match value {
		Value::RecordId(mut rid) => {
			if let crate::val::RecordIdKey::String(key) = &rid.key
				&& key.len() == 20
			{
				rid.key = crate::val::RecordIdKey::String("generated".into());
			}
			Value::RecordId(rid)
		}
		Value::Array(array) => {
			Value::Array(array.into_iter().map(without_generated_ids).collect::<Vec<_>>().into())
		}
		Value::Object(object) => Value::Object(
			object
				.into_iter()
				.map(|(k, v)| (k, without_generated_ids(v)))
				.collect::<std::collections::BTreeMap<_, _>>()
				.into(),
		),
		other => other,
	}
}

/// Run `sql` on a fresh database and answer its rows and the records left in
/// every table of [`WRITE_TABLES`].
async fn written(sql: &str) -> (Result<Vec<Value>, String>, Vec<String>) {
	let db = TestDb::new(WRITE_SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let rows =
		outcome(&op, &ctx).await.map(|rows| rows.into_iter().map(without_generated_ids).collect());
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let read = db.exec_ctx().await;
	let mut state = Vec::new();
	for table in WRITE_TABLES {
		let op = crate::exec::operators::test_util::plan_statement(
			&format!("SELECT * FROM {table}"),
			&read,
		)
		.await;
		let mut rows: Vec<String> = outcome(&op, &read)
			.await
			.expect("the state reads")
			.into_iter()
			.map(|row| format!("{table}: {}", without_generated_ids(row).to_sql()))
			.collect();
		rows.sort();
		state.extend(rows);
	}
	(rows, state)
}

/// A write's rows or error and the records it leaves, as a golden body.
pub(super) fn render_written(rows: &Result<Vec<Value>, String>, state: &[String]) -> String {
	format!("{}\nrecords:\n{}", render_outcome(rows), state.join("\n"))
}

#[tokio::test]
async fn a_write_answers_its_rows_and_leaves_its_records() {
	let mut golden = Golden::new("a_write_answers_its_rows_and_leaves_its_records");
	for sql in WRITES {
		let (rows, state) = written(sql).await;
		golden.record(sql, render_written(&rows, &state));
	}
	golden.check();
}

/// A table read as a region answers what the serial run answers — its rows in
/// order, and the failure of the earliest failing batch — whichever of its
/// batches the workers took.
#[tokio::test]
async fn a_region_answers_what_the_serial_run_answers() {
	let setup = "
		DEFINE TABLE t;
		DEFINE FIELD twice ON t COMPUTED n * 2;
		DEFINE INDEX by_n ON t FIELDS n;
		DEFINE INDEX by_m ON t FIELDS m;
		FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = $i, m = $i % 10, s = <string> $i };
		UPDATE t:2500 SET s = 'x';
	";
	let mut golden = Golden::new("a_region_answers_what_the_serial_run_answers");
	let mut serial = Vec::new();
	let config = surrealdb_cnf::ConfigMap::empty().with_key_value("region_row_threshold", "1");
	let handed = super::region::HANDED_TO_WORKERS.load(std::sync::atomic::Ordering::Relaxed);
	for (label, db) in [
		("serial", TestDb::new(setup).await),
		("region", TestDb::new_with_config(setup, config).await),
	] {
		let ctx = db.exec_ctx().await;
		// Whether each runs as a region: a body the storage cursor runs in
		// place reads rows too cheap to be worth a worker, and is none; a
		// table's layer lands through the host, so its rows are decoded.
		for (at, (sql, region)) in [
			("SELECT n, twice FROM t", true),
			// A statement nested per row that only reads runs over each
			// batch's state; one kept from its first evaluation runs serially.
			(
				"SELECT n, (SELECT VALUE twice FROM ONLY $parent.id) AS again FROM t WHERE n % 7 = 0",
				true,
			),
			(
				"SELECT n, (SELECT VALUE twice FROM ONLY $parent.id) AS again FROM t LIMIT 40 START 3",
				true,
			),
			("SELECT n, (SELECT VALUE count() FROM t GROUP ALL) AS all FROM t", false),
			("SELECT VALUE $this.n + twice FROM t", true),
			("SELECT n, <int> s AS i FROM t", false),
			("SELECT n, $this.s.len() AS l, <int> $this.s AS i FROM t", true),
			// An index, a record range and a union of indexes, in their order.
			("SELECT n, $this.s AS s FROM t WHERE n > 1200", true),
			("SELECT n, $this.s AS s FROM t WHERE n > 1200 ORDER BY n DESC", true),
			("SELECT n, $this.s AS s FROM t:100..2900", true),
			("SELECT n, $this.s AS s FROM t WHERE n < 50 OR m = 3", true),
			("SELECT n, <int> $this.s AS i FROM t WHERE n > 2000", true),
			// Sorts, whole and under a limit, over keys most rows tie on.
			("SELECT n, $this.m AS k FROM t ORDER BY k", true),
			("SELECT n, $this.m AS k FROM t ORDER BY k DESC LIMIT 7", true),
			("SELECT n, $this.m AS k FROM t ORDER BY k, twice DESC LIMIT 25 START 3", true),
			// Aggregates whose partial states combine exactly; reading `twice`
			// decodes the rows, through its table's layer.
			(
				"SELECT m, count() AS c, array::group(twice % 3) AS g, math::max(n) AS mx, \
				 array::join(s, ',') AS all, math::median(twice) AS md FROM t GROUP BY m",
				true,
			),
			(
				"SELECT count() AS c, array::distinct(m) AS d, math::min(twice) AS mn FROM t GROUP ALL",
				true,
			),
			("SELECT m, s AS first, math::max(twice) AS top FROM t GROUP BY m", true),
			// A float sum depends on how it is grouped, so it folds serially.
			("SELECT m, math::sum(twice) AS total FROM t GROUP BY m", false),
		]
		.into_iter()
		.enumerate()
		{
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
			let answer = outcome(&vm, &ctx).await;
			if label == "serial" {
				golden.rows(sql, &answer);
				serial.push(answer);
			} else {
				assert_eq!(answer, serial[at], "`{sql}` as a region");
			}
			let physical = super::optimize::optimize(
				super::map::map(&planner(&ctx), &bound).await.expect("maps"),
			);
			let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
			assert_eq!(program.pipelines.iter().any(|p| p.region), region, "`{sql}`");
		}
	}
	assert!(
		super::region::HANDED_TO_WORKERS.load(std::sync::atomic::Ordering::Relaxed) > handed,
		"no batch ran on a worker"
	);
	golden.check();
}

/// A paged body read as a region answers what the VM's serial run answers:
/// the rows `START` and `LIMIT` leave, the failure of a row the serial run
/// reaches, and no failure of a row past the limit, even one a batch ran
/// ahead of the merge.
#[tokio::test(flavor = "multi_thread")]
async fn a_paged_region_answers_what_the_serial_run_answers() {
	let setup = "
		DEFINE TABLE t;
		DEFINE INDEX by_n ON t FIELDS n;
		FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = $i, s = <string> $i };
		UPDATE t:2500 SET s = 'x';
	";
	let config = surrealdb_cnf::ConfigMap::empty().with_key_value("region_row_threshold", "1");
	let serial = TestDb::new_with_config(
		setup,
		surrealdb_cnf::ConfigMap::empty()
			.with_key_value("region_row_threshold", usize::MAX.to_string()),
	)
	.await;
	let region = TestDb::new_with_config(setup, config).await;
	// Rows 0, 100, …, 2400 and then every row past 2450 pass the filter, so
	// the merged batches show a filter passing few rows and the window widens
	// past the rows the limit leaves; row 2500 fails `<int>`.
	let sparse = "(n % 100 = 0 OR n > 2450)";
	let cases = [
		("SELECT n, $this.s AS s FROM t LIMIT 1000".to_owned(), true, Ok(1000)),
		("SELECT n, $this.s AS s FROM t LIMIT 1".to_owned(), true, Ok(1)),
		("SELECT n, $this.s AS s FROM t LIMIT 10 START 5".to_owned(), true, Ok(10)),
		("SELECT n, $this.s AS s FROM t START 2990".to_owned(), true, Ok(10)),
		("SELECT n, $this.s AS s FROM t LIMIT 0".to_owned(), true, Ok(0)),
		("SELECT n, $this.s AS s FROM t WHERE n % 97 = 0 LIMIT 20".to_owned(), true, Ok(20)),
		("SELECT n, $this.s AS s FROM t WHERE n > 1200 LIMIT 50 START 3".to_owned(), true, Ok(50)),
		// A projection that fails: before the limit, at a row taken, and at a
		// row skipped or past the limit, which the serial run never projects.
		("SELECT n, <int> $this.s AS i FROM t LIMIT 2400".to_owned(), true, Ok(2400)),
		("SELECT n, <int> $this.s AS i FROM t LIMIT 2600".to_owned(), true, Err(())),
		("SELECT n, <int> $this.s AS i FROM t LIMIT 10 START 2495".to_owned(), true, Err(())),
		("SELECT n, <int> $this.s AS i FROM t LIMIT 5 START 2501".to_owned(), true, Ok(5)),
		("SELECT n, <int> $this.s AS i FROM t LIMIT 0".to_owned(), true, Ok(0)),
		(format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 30"), true, Ok(30)),
		(format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 80"), true, Err(())),
		// `START` past rows a batch runs ahead: skipped up to row 2500, and
		// taking it first or last.
		(
			format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 5 START 75"),
			true,
			Ok(5),
		),
		(
			format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 5 START 74"),
			true,
			Err(()),
		),
		(
			format!("SELECT n, <int> $this.s AS i FROM t WHERE {sparse} LIMIT 5 START 70"),
			true,
			Err(()),
		),
		(format!("SELECT n, $this.s AS s FROM t WHERE {sparse} LIMIT 400 START 26"), true, Ok(400)),
		// A filter that fails: the serial run judges every row it reads,
		// skipped or taken, and none past the limit.
		(
			format!("SELECT n, $this.s AS s FROM t WHERE {sparse} AND <int> $this.s >= 0 LIMIT 30"),
			true,
			Ok(30),
		),
		(
			format!("SELECT n, $this.s AS s FROM t WHERE {sparse} AND <int> $this.s >= 0 LIMIT 80"),
			true,
			Err(()),
		),
		(
			"SELECT n, $this.s AS s FROM t WHERE <int> $this.s >= 0 LIMIT 5 START 2600".to_owned(),
			true,
			Err(()),
		),
		("SELECT n, $this.s AS s FROM t WHERE <int> $this.s >= 0 LIMIT 0".to_owned(), true, Ok(0)),
	];
	let handed = super::region::HANDED_TO_WORKERS.load(std::sync::atomic::Ordering::Relaxed);
	for (sql, is_region, expected) in &cases {
		let serial_ctx = serial.exec_ctx().await;
		let bound = bind(&serial_ctx, sql).await;
		let vm = super::plan(&planner(&serial_ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		let answer = outcome(&vm, &serial_ctx).await;
		assert_eq!(
			answer.as_ref().map(Vec::len).map_err(|_| ()),
			*expected,
			"`{sql}` run serially: {answer:?}"
		);
		let ctx = region.exec_ctx().await;
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		assert_eq!(outcome(&vm, &ctx).await, answer, "`{sql}` as a region");
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		assert_eq!(program.pipelines.iter().any(|p| p.region), *is_region, "`{sql}`");
	}
	assert!(
		super::region::HANDED_TO_WORKERS.load(std::sync::atomic::Ordering::Relaxed) > handed,
		"no paged batch ran on a worker"
	);
}

/// How long a region takes over a table whose rows each cost real work, run
/// serially and with workers. Run with
/// `cargo test --release -p surrealdb-core --lib region_throughput -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a check"]
async fn region_throughput() {
	let setup = "
		DEFINE TABLE t;
		DEFINE FIELD h ON t COMPUTED crypto::sha512(string::repeat(s, 400));
		FOR $i IN 0..20000 { CREATE type::record('t', $i) SET s = <string> $i, m = $i % 16 };
	";
	for sql in [
		"SELECT VALUE crypto::sha512(string::repeat($this.s, 400)) FROM t",
		"SELECT s, h FROM t ORDER BY h LIMIT 10",
		"SELECT m, count() AS c, array::max(h) AS top FROM t GROUP BY m",
	] {
		for (label, threshold) in
			[("serial", usize::MAX.to_string()), ("region", "256".to_string())]
		{
			let config =
				surrealdb_cnf::ConfigMap::empty().with_key_value("region_row_threshold", threshold);
			let db = TestDb::new_with_config(setup, config).await;
			let ctx = db.exec_ctx().await;
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
			let mut best = std::time::Duration::MAX;
			for _ in 0..3 {
				let started = web_time::Instant::now();
				outcome(&vm, &ctx).await.expect("runs");
				best = best.min(started.elapsed());
			}
			eprintln!("{sql} — {label}: {best:?}");
		}
	}
}

/// How long dense graph lookups take under and without a `LIMIT`: as
/// configured, without regions, and with neither regions nor fan-out. Run with
/// `cargo test --release -p surrealdb-core --lib graph_limit_throughput
/// -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a check"]
async fn graph_limit_throughput() {
	let setup = "
		DEFINE TABLE person SCHEMALESS;
		CREATE |person:1..=5000| SET name = rand::string(12), age = rand::int(18, 80) RETURN NONE;
		FOR $p IN (SELECT VALUE id FROM person) {
			FOR $_ IN array::range(1, 10) {
				LET $t = type::record('person', rand::int(1, 5000));
				RELATE $p->knows->$t;
			};
		};
	";
	let serial = surrealdb_cnf::ConfigMap::empty()
		.with_key_value("region_row_threshold", usize::MAX.to_string());
	let none = surrealdb_cnf::ConfigMap::empty()
		.with_key_value("region_row_threshold", usize::MAX.to_string())
		.with_key_value("fan_out_row_threshold", usize::MAX.to_string());
	for (label, db) in [
		("default", TestDb::new(setup).await),
		("no regions", TestDb::new_with_config(setup, serial).await),
		("no fan-out", TestDb::new_with_config(setup, none).await),
	] {
		let ctx = db.exec_ctx().await;
		for sql in [
			"SELECT ->knows->person->knows->person AS fof FROM person LIMIT 1000",
			"SELECT ->knows->person->knows->person AS fof FROM person",
			"SELECT ->knows->(person WHERE age > 30) AS friends FROM person LIMIT 1000",
			"SELECT ->knows->(person WHERE age > 30) AS friends FROM person",
		] {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
			let mut best = std::time::Duration::MAX;
			for _ in 0..5 {
				let started = web_time::Instant::now();
				outcome(&vm, &ctx).await.expect("runs");
				best = best.min(started.elapsed());
			}
			eprintln!("{label} | {sql}: {best:?}");
		}
	}
}

/// How long `ORDER BY … LIMIT` takes. Run with `cargo test --release -p surrealdb-core --lib
/// order_limit_throughput -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn order_limit_throughput() {
	let setup = "
		DEFINE TABLE record;
		FOR $i IN 0..10000 {
			CREATE type::record('record', $i) SET score = rand::int(0, 1000000), name = <string> $i, tags = ['a', 'b'], nested = { a: $i, b: 'x' }
		};
	";
	let db = TestDb::new(setup).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT * FROM record ORDER BY score DESC LIMIT 100",
		"SELECT name FROM record ORDER BY score LIMIT 10 START 5",
		"SELECT * FROM record WHERE score < 0",
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
		let mut best = std::time::Duration::MAX;
		for _ in 0..5 {
			let started = web_time::Instant::now();
			outcome(&vm, &ctx).await.expect("runs");
			best = best.min(started.elapsed());
		}
		eprintln!("{sql}: {best:?}");
	}
}

/// How long the shapes of the read regressions take. Run with `cargo test --release -p
/// surrealdb-core --lib read_regressions_throughput -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn read_regressions_throughput() {
	let base = "
		DEFINE TABLE record SCHEMALESS PERMISSIONS FOR select, create, update, delete FULL;
		CREATE |record:10000| SET
			name = rand::string(20), age = rand::int(1, 100),
			city = rand::enum('London', 'Paris', 'Berlin'), score = rand::float(0.0, 100.0),
			number = rand::int(1, 5000), status = rand::enum('draft', 'published'),
			words = array::join(array::map(array::range(0, 24), |$i| rand::enum('hello', 'world', 'foo')), ' '),
			tags = ['a', 'b', 'c'], nested = { a: { b: { c: rand::string(10) } } },
			description = rand::string(100, 400);
		DEFINE TABLE user SCHEMALESS;
		CREATE user:bench SET name = 'bench';
		DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( SELECT * FROM type::record('user', $id) );
	";
	let computed = "
		DEFINE FIELD label ON record TYPE string COMPUTED string::concat(name, ' / ', city);
		DEFINE FIELD bucket ON record TYPE number COMPUTED math::floor(score / 10);
		DEFINE FIELD word_count ON record TYPE int COMPUTED array::len(string::split(words, ' '));
		DEFINE FIELD status_tag ON record COMPUTED string::concat(city ?? 'unknown', ' / ', status ?? 'unknown');
	";
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "bench"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (label, setup, session, sql) in [
		(
			"R4 range",
			base.to_owned(),
			TestDb::owner(),
			"SELECT * FROM record WHERE number >= 18 AND number <= 21",
		),
		("R4 eq", base.to_owned(), TestDb::owner(), "SELECT * FROM record WHERE number = 18"),
		(
			"R5 static",
			format!(
				"{base}{computed} DEFINE FIELD name ON record PERMISSIONS FOR select WHERE age < 200;"
			),
			reader.clone(),
			"SELECT name FROM record",
		),
		(
			"R5 opaque",
			format!(
				"{base}{computed} DEFINE FIELD name ON record PERMISSIONS FOR select WHERE $auth != NONE;"
			),
			reader.clone(),
			"SELECT name FROM record",
		),
	] {
		let db = TestDb::new_with_auth(&setup).await;
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
		let mut best = std::time::Duration::MAX;
		for _ in 0..7 {
			let started = web_time::Instant::now();
			outcome(&vm, &ctx).await.expect("runs");
			best = best.min(started.elapsed());
		}
		eprintln!("{label} — run: {best:?}");
		// Planning included, as a statement pays it each time it is sent.
		let mut best = std::time::Duration::MAX;
		for _ in 0..7 {
			let started = web_time::Instant::now();
			let bound = bind(&ctx, sql).await;
			let op = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
			outcome(&op, &ctx).await.expect("runs");
			best = best.min(started.elapsed());
		}
		eprintln!("{label} — plan+run: {best:?}");
	}
}

/// How long the shapes that run a unit per row take: a correlated subquery in a projection, a
/// `FETCH` through the table's dispatch unit, with and without a computed field for the unit
/// to derive, a closure body per element that calls a
/// user-defined function and one that does not, a function call per row,
/// and a dynamic read of a table the run itself defined under a `LIMIT`. Run
/// with `cargo test --release -p surrealdb-core --lib unit_invocation_throughput
/// -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn unit_invocation_throughput() {
	let setup = "
		DEFINE TABLE person SCHEMALESS; DEFINE TABLE post SCHEMALESS;
		DEFINE FUNCTION fn::inc($x: int) { RETURN $x + 1 };
		FOR $i IN 0..200 { CREATE type::record('person', $i) SET name = <string> $i, age = $i };
		FOR $i IN 0..10000 {
			CREATE type::record('post', $i) SET author = type::record('person', $i % 200), n = $i,
				score = rand::int(0, 1000000), tags = [1, 2, 3]
		};
	";
	let db = TestDb::new(setup).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT n, (SELECT VALUE name FROM ONLY $parent.author) AS an FROM post",
		"SELECT * FROM post FETCH author",
		"SELECT n, author.name AS an FROM post",
		"SELECT n, array::map(tags, |$t| fn::inc($t)) AS m FROM post",
		"SELECT n, array::map(tags, |$t| $t + n) AS m FROM post",
		"SELECT n, fn::inc(n) AS m FROM post",
		"SELECT * FROM post ORDER BY score DESC LIMIT 100",
		"SELECT * FROM post WHERE array::len(tags) = 3 AND n > 100",
	] {
		on_the_vm(&ctx, sql).await;
	}
	// A `FETCH` whose table's unit lands a layer, a computed field, on every
	// record it reads.
	let db = TestDb::new(
		"
		DEFINE TABLE pet SCHEMALESS; DEFINE TABLE post SCHEMALESS;
		DEFINE FIELD label ON pet COMPUTED 'pet ' + name;
		FOR $i IN 0..200 { CREATE type::record('pet', $i) SET name = <string> $i };
		FOR $i IN 0..10000 { CREATE type::record('post', $i) SET pet = type::record('pet', $i % 200), n = $i };
		",
	)
	.await;
	on_the_vm(&db.exec_ctx().await, "SELECT * FROM post FETCH pet").await;
	// A table the catalog did not hold when the statement was lowered, which
	// a later write defined: the program holds no unit for it.
	let db = TestDb::new("DEFINE TABLE other SCHEMALESS;").await;
	let ctx = db.exec_ctx().await;
	let sql = "SELECT * FROM type::table('fresh') LIMIT 10";
	let bound = bind(&ctx, sql).await;
	let vm = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
	db.datastore()
		.execute("CREATE |fresh:1..=20000| SET n = 1 RETURN NONE;", &TestDb::owner(), None)
		.await
		.expect("creates");
	let ctx = db.exec_ctx().await;
	let mut best = std::time::Duration::MAX;
	for _ in 0..9 {
		let started = web_time::Instant::now();
		let rows = outcome(&vm, &ctx).await.expect("runs");
		best = best.min(started.elapsed());
		assert_eq!(rows.len(), 10);
	}
	eprintln!("{sql} — vm: {best:?}");
}

/// The best of nine runs of `sql` in `ctx` on the VM, printed.
async fn on_the_vm(ctx: &ExecutionContext, sql: &str) {
	let bound = bind(ctx, sql).await;
	let vm = super::plan(&planner(ctx), &bound).await.expect("the VM runs it");
	let mut best = std::time::Duration::MAX;
	for _ in 0..9 {
		let started = web_time::Instant::now();
		outcome(&vm, ctx).await.expect("runs");
		best = best.min(started.elapsed());
	}
	eprintln!("{sql}: {best:?}");
}

/// One of the shapes [`unit_invocation_throughput`] measures run over and
/// over, for a profiler to sample. Run with `cargo test --release -p
/// surrealdb-core --lib unit_invocation_profile -- --ignored` under the
/// profiler; `UNIT_PROFILE_SQL` names the statement.
#[tokio::test]
#[ignore = "a profiling load, not a check"]
async fn unit_invocation_profile() {
	let setup = "
		DEFINE TABLE person SCHEMALESS; DEFINE TABLE post SCHEMALESS;
		DEFINE FUNCTION fn::inc($x: int) { RETURN $x + 1 };
		FOR $i IN 0..200 { CREATE type::record('person', $i) SET name = <string> $i, age = $i };
		FOR $i IN 0..10000 {
			CREATE type::record('post', $i) SET author = type::record('person', $i % 200), n = $i,
				score = rand::int(0, 1000000), tags = [1, 2, 3]
		};
	";
	let db = TestDb::new(setup).await;
	let ctx = db.exec_ctx().await;
	let sql = std::env::var("UNIT_PROFILE_SQL")
		.unwrap_or_else(|_| "SELECT * FROM post FETCH author".to_owned());
	let bound = bind(&ctx, &sql).await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
	let started = web_time::Instant::now();
	for _ in 0..300 {
		outcome(&op, &ctx).await.expect("runs");
	}
	eprintln!("300 runs: {:?}", started.elapsed());
}

/// An in-place scan run over and over, for a profiler to sample. Run with
/// `cargo test --release -p surrealdb-core --lib scan_profile -- --ignored`
/// under the profiler; `SCAN_PROFILE_SQL` names another statement over the same
/// table.
#[tokio::test]
#[ignore = "a profiling load, not a check"]
async fn scan_profile() {
	let setup = "
		DEFINE TABLE record;
		FOR $i IN 0..10000 {
			CREATE type::record('record', $i) SET score = rand::int(0, 1000000), name = <string> $i, tags = ['a', 'b'], nested = { a: $i, b: 'x' }
		};
	";
	let db = TestDb::new(setup).await;
	let ctx = db.exec_ctx().await;
	let sql = std::env::var("SCAN_PROFILE_SQL")
		.unwrap_or_else(|_| "SELECT * FROM record WHERE score < 0".to_owned());
	let bound = bind(&ctx, &sql).await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
	let started = web_time::Instant::now();
	for _ in 0..3000 {
		outcome(&op, &ctx).await.expect("runs");
	}
	eprintln!("3000 scans: {:?}", started.elapsed());
}

/// The body of [`write_profile_entry`].
fn write_profile() {
	let sql = std::env::var("WRITE_PROFILE_SQL")
		.unwrap_or_else(|_| "RELATE (SELECT VALUE id FROM t)->e->t:0 RETURN NONE".to_owned());
	let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	rt.block_on(async {
		let setup = "
			DEFINE TABLE t;
			DEFINE FIELD n ON t TYPE int;
			FOR $i IN 0..1000 { CREATE type::record('t', $i) SET n = $i };
		";
		let mut spent = std::time::Duration::ZERO;
		let db = TestDb::new(setup).await;
		for _ in 0..3000 {
			let session = TestDb::owner();
			let started = web_time::Instant::now();
			let mut results = db.datastore().execute(&sql, &session, None).await.expect("executes");
			spent += started.elapsed();
			assert!(results.remove(0).result.is_ok());
		}
		eprintln!("3000 runs: {spent:?}");
	})
}

/// A write run over and over, for a profiler to sample. Run with
/// `cargo test --profile profiling -p surrealdb-core --lib write_profile --
/// --ignored` under the profiler; `WRITE_PROFILE_SQL` names another statement.
#[test]
#[ignore = "a profiling load, not a check"]
fn write_profile_entry() {
	write_profile();
}

/// How long writes take, each statement run whole through the datastore on a
/// fresh database. Run with
/// `cargo test --release -p surrealdb-core --lib write_throughput -- --ignored
/// --nocapture`.
#[test]
#[ignore = "a measurement, not a check"]
fn write_throughput() {
	let setup = "
		DEFINE TABLE t;
		DEFINE FIELD n ON t TYPE int;
		FOR $i IN 0..1000 { CREATE type::record('t', $i) SET n = $i };
		DEFINE TABLE it;
		DEFINE INDEX it_n ON it FIELDS n;
		FOR $i IN 0..1000 { CREATE type::record('it', $i) SET n = $i };
		DEFINE TABLE ix;
		DEFINE INDEX ix_n ON ix FIELDS n;
		DEFINE TABLE ux;
		DEFINE INDEX ux_n ON ux FIELDS n UNIQUE;
	";
	let shapes = [
		("index_insert", "INSERT INTO ix (SELECT n FROM t)"),
		("index_insert_unique", "INSERT INTO ux (SELECT n FROM t)"),
		("index_update", "UPDATE it SET n += 1"),
		("index_delete", "DELETE it"),
		("update", "UPDATE t SET n += 1"),
		("delete", "DELETE t"),
		("update_where", "UPDATE t SET n += 1 WHERE n % 2 = 0"),
		("insert", "INSERT INTO u (SELECT n FROM t)"),
		("relate", "RELATE (SELECT VALUE id FROM t)->e->t:0"),
		("for_create_table", "FOR $i IN 0..1000 { CREATE v SET n = $i }"),
		("for_upsert_record", "FOR $i IN 0..1000 { UPSERT type::record('v', $i) SET n = $i }"),
		("for_create_record", "FOR $i IN 0..1000 { CREATE type::record('w', $i) SET n = $i }"),
		// The language-test benches `crud/loop_create_1000` and
		// `crud/loop_upsert_1000`.
		(
			"loop_create_1000",
			"FOR $i IN 0..1000 { CREATE record SET n = $i, name = <string> $i, active = $i % 2 = 0 }",
		),
		(
			"loop_upsert_1000",
			"FOR $i IN 0..1000 { UPSERT type::record('record', $i) SET n = $i, name = <string> $i }",
		),
	];
	let filter = std::env::var("WRITE_SHAPES").ok();
	for (name, sql) in shapes {
		if filter.as_deref().is_some_and(|f| !f.split(',').any(|s| s == name)) {
			continue;
		}
		let mut line = format!("{name}:");
		{
			let best = {
				let rt = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("a runtime");
				rt.block_on(async {
					let mut best = std::time::Duration::MAX;
					for _ in 0..5 {
						let db = TestDb::new(setup).await;
						let session = TestDb::owner();
						let started = web_time::Instant::now();
						let mut results =
							db.datastore().execute(sql, &session, None).await.expect("executes");
						let elapsed = started.elapsed();
						let result = results.remove(0).result;
						assert!(result.is_ok(), "`{sql}`: {result:?}");
						best = best.min(elapsed);
					}
					best
				})
			};
			line.push_str(&format!(" {best:?}"));
		}
		eprintln!("{line}");
	}
}

/// How long writes to a table live subscriptions watch take: no subscription, one the owner opened,
/// one a record user opened over a table with a field it may not select and a `COMPUTED` field,
/// with a `WHERE` and a projection, and ten of each. Each statement runs whole
/// through the datastore on a fresh database, the subscriptions opened first,
/// its notifications drained as they are sent. Run with `cargo test --release
/// -p surrealdb-core --lib live_write_throughput -- --ignored --nocapture`;
/// `WRITE_SHAPES` narrows it.
#[test]
#[ignore = "a measurement, not a check"]
fn live_write_throughput() {
	let setup = "
		DEFINE TABLE user PERMISSIONS FOR select FULL;
		CREATE user:reader;
		DEFINE TABLE t PERMISSIONS FOR select WHERE hidden != true, FOR create, update, delete FULL;
		DEFINE FIELD n ON t TYPE int;
		DEFINE FIELD secret ON t PERMISSIONS FOR select WHERE $auth.id = user:x;
		DEFINE FIELD double ON t COMPUTED n * 2;
		FOR $i IN 0..1000 { CREATE type::record('t', $i) SET n = $i, secret = 's' };
	";
	let owner_live = "LIVE SELECT * FROM t";
	let reader_live = "LIVE SELECT n, double, secret FROM t WHERE n % 2 = 0";
	let watchers: [(&str, &[(bool, &str)]); 5] = [
		("none", &[]),
		("owner", &[(false, owner_live)]),
		("reader", &[(true, reader_live)]),
		("owner_x10", &[(false, owner_live); 10]),
		("reader_x10", &[(true, reader_live); 10]),
	];
	let writes = [
		("update", "UPDATE t SET n += 1"),
		(
			"for_create",
			"FOR $i IN 0..1000 { CREATE type::record('v', $i) SET n = $i }; UPDATE t:0 SET n = 0",
		),
	];
	let filter = std::env::var("WRITE_SHAPES").ok();
	for (write, sql) in writes {
		for (watching, lives) in watchers {
			let name = format!("{write}/{watching}");
			if filter.as_deref().is_some_and(|f| !f.split(',').any(|s| s == name)) {
				continue;
			}
			let mut line = format!("{name}:");
			{
				let best = {
					let rt = tokio::runtime::Builder::new_current_thread()
						.enable_all()
						.build()
						.expect("a runtime");
					rt.block_on(async {
						let mut best = std::time::Duration::MAX;
						for _ in 0..5 {
							let (db, notifications) = TestDb::new_with_notifications(setup).await;
							let drain =
								tokio::spawn(
									async move { while notifications.recv().await.is_ok() {} },
								);
							let rid = crate::types::PublicValue::RecordId(
								surrealdb_types::RecordId::new("user", "reader"),
							);
							let reader =
								crate::dbs::Session::for_record("test", "test", "user", rid)
									.with_rt(true);
							for (as_reader, live) in lives.iter() {
								let session = if *as_reader {
									reader.clone()
								} else {
									TestDb::owner().with_rt(true)
								};
								let mut responses = db
									.datastore()
									.execute(live, &session, None)
									.await
									.expect("opens");
								assert!(responses.remove(0).result.is_ok());
							}
							let session = TestDb::owner();
							let started = web_time::Instant::now();
							let results = db
								.datastore()
								.execute(sql, &session, None)
								.await
								.expect("executes");
							let elapsed = started.elapsed();
							for result in results {
								assert!(result.result.is_ok(), "`{sql}`");
							}
							best = best.min(elapsed);
							drop(db);
							drain.abort();
						}
						best
					})
				};
				line.push_str(&format!(" {best:?}"));
			}
			eprintln!("{line}");
		}
	}
}

/// How long writes whose records reach nested writes take: a delete's cascades over referencing
/// records and edges, an event's statement, and a view table's rows kept up to date. Each statement
/// runs whole through the datastore on a fresh database. Run with `cargo test --release -p
/// surrealdb-core --lib nested_write_throughput -- --ignored --nocapture`;
/// `WRITE_SHAPES` and `WRITE_RUNS` narrow it.
#[test]
#[ignore = "a measurement, not a check"]
fn nested_write_throughput() {
	let parents = "
		DEFINE TABLE p;
		FOR $i IN 0..1000 { CREATE type::record('p', $i) };
	";
	let shapes = [
		(
			"delete_cascade",
			"DEFINE FIELD owner ON c TYPE option<record<p>> REFERENCE ON DELETE CASCADE;
			FOR $i IN 0..1000 { CREATE type::record('c', $i) SET owner = type::record('p', $i) };",
			"DELETE p",
		),
		(
			"for_delete_cascade",
			"DEFINE FIELD owner ON c TYPE option<record<p>> REFERENCE ON DELETE CASCADE;
			FOR $i IN 0..1000 { CREATE type::record('c', $i) SET owner = type::record('p', $i) };",
			"FOR $i IN 0..1000 { DELETE type::record('p', $i) }",
		),
		(
			"delete_unset",
			"DEFINE FIELD owner ON c TYPE option<record<p>> REFERENCE ON DELETE UNSET;
			FOR $i IN 0..1000 { CREATE type::record('c', $i) SET owner = type::record('p', $i) };",
			"DELETE p",
		),
		(
			"delete_edges",
			"FOR $i IN 0..1000 { LET $a = type::record('p', $i); LET $b = type::record('p', ($i + 1) % 1000); RELATE $a->e->$b };",
			"DELETE p",
		),
		(
			"event_then",
			"DEFINE TABLE src; DEFINE TABLE log;
			DEFINE EVENT logged ON src THEN (CREATE log SET of = $after.id, v = $after.v);",
			"FOR $i IN 0..1000 { CREATE src SET g = $i % 10, v = $i }",
		),
		(
			"view_insert",
			"DEFINE TABLE src; DEFINE TABLE s AS SELECT g, count() AS n, math::sum(v) AS total FROM src GROUP BY g;",
			"FOR $i IN 0..1000 { CREATE src SET g = $i % 10, v = $i }",
		),
		(
			"view_delete_extremes",
			"DEFINE TABLE src; DEFINE TABLE s AS SELECT g, math::max(v) AS hi, math::min(v) AS lo FROM src GROUP BY g;
			FOR $i IN 0..1000 { CREATE type::record('src', $i) SET g = $i % 10, v = $i };",
			"DELETE src",
		),
		(
			"delete_then",
			"DEFINE FIELD owner ON c TYPE option<record<p>> REFERENCE ON DELETE THEN (UPDATE $this SET gone = true);
			FOR $i IN 0..1000 { CREATE type::record('c', $i) SET owner = type::record('p', $i) };",
			"DELETE p",
		),
		(
			"view_materialized",
			"DEFINE TABLE src; DEFINE TABLE m AS SELECT g, v, v * 2 AS w FROM src WHERE v > 10;",
			"FOR $i IN 0..1000 { CREATE src SET g = $i % 10, v = $i }",
		),
		(
			"view_update",
			"DEFINE TABLE src; DEFINE TABLE s AS SELECT g, count() AS n, math::sum(v) AS total FROM src GROUP BY g;
			FOR $i IN 0..1000 { CREATE type::record('src', $i) SET g = $i % 10, v = $i };",
			"UPDATE src SET v += 1",
		),
		(
			"view_of_view",
			"DEFINE TABLE src; DEFINE TABLE m AS SELECT g, v FROM src WHERE v > 10;
			DEFINE TABLE s AS SELECT g, count() AS n, math::sum(v) AS total FROM m GROUP BY g;",
			"FOR $i IN 0..1000 { CREATE src SET g = $i % 10, v = $i }",
		),
	];
	let filter = std::env::var("WRITE_SHAPES").ok();
	let runs = std::env::var("WRITE_RUNS").ok().and_then(|runs| runs.parse().ok()).unwrap_or(5);
	for (name, setup, sql) in shapes {
		if filter.as_deref().is_some_and(|f| !f.split(',').any(|s| s == name)) {
			continue;
		}
		let setup = format!("{parents}{setup}");
		let mut line = format!("{name}:");
		{
			let best = {
				let rt = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("a runtime");
				rt.block_on(async {
					let mut best = std::time::Duration::MAX;
					for _ in 0..runs {
						let db = TestDb::new(&setup).await;
						let session = TestDb::owner();
						let started = web_time::Instant::now();
						let mut results =
							db.datastore().execute(sql, &session, None).await.expect("executes");
						let elapsed = started.elapsed();
						let result = results.remove(0).result;
						assert!(result.is_ok(), "`{sql}`: {result:?}");
						best = best.min(elapsed);
					}
					best
				})
			};
			line.push_str(&format!(" {best:?}"));
		}
		eprintln!("{line}");
	}
}

/// A `FOR` loop of writes run over and over, for a profiler to sample. Run with
/// `cargo test --profile profiling -p surrealdb-core --lib loop_profile --
/// --ignored` under the profiler; `LOOP_PROFILE_SQL` names another statement.
#[test]
#[ignore = "a profiling load, not a check"]
fn loop_profile() {
	let sql = std::env::var("LOOP_PROFILE_SQL")
		.unwrap_or_else(|_| "FOR $i IN 0..1000 { CREATE v SET n = $i }".to_owned());
	let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	let started = rt.block_on(async {
		let iterations: usize =
			std::env::var("LOOP_PROFILE_ITERS").ok().and_then(|n| n.parse().ok()).unwrap_or(200);
		let started = web_time::Instant::now();
		for _ in 0..iterations {
			let db = TestDb::new("DEFINE TABLE t;").await;
			let session = TestDb::owner();
			let mut results = db.datastore().execute(&sql, &session, None).await.expect("executes");
			assert!(results.remove(0).result.is_ok());
		}
		started.elapsed()
	});
	eprintln!("loops: {started:?}");
}

/// How long planning a write takes. Run with `cargo test --release -p surrealdb-core --lib
/// write_planning -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn write_planning() {
	let db =
		TestDb::new("DEFINE TABLE t; DEFINE FIELD n ON t TYPE int; CREATE t:1 SET n = 1;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for sql in [
		"CREATE t SET n = 1",
		"UPSERT type::record('t', 5) SET n = 1",
		"CREATE type::record('w', 5) SET n = 1",
		"UPDATE t SET n += 1",
	] {
		let op = parse(sql);
		let mut line = format!("{sql}:");
		for (label, plans) in [("bind", false), ("vm", true)] {
			let started = web_time::Instant::now();
			for _ in 0..1000 {
				let bound = bind_parsed(&ctx, op.clone()).await;
				if plans {
					super::plan(&planner(&ctx), &bound).await.expect("the VM plans it");
				}
			}
			line.push_str(&format!(" {label} {:?}", started.elapsed() / 1000));
		}
		eprintln!("{line}");
	}
}

/// A sort that asks for `TEMPFILES` holds on disk what outgrows its buffer, and
/// answers its rows in the same order — ties included — whether they are
/// merged from runs or not.
#[cfg(all(storage, not(target_family = "wasm")))]
#[tokio::test]
async fn a_spilling_sort_orders_its_rows_ties_included() {
	let dir = tempfile::tempdir().expect("a temporary directory");
	let setup = "
		DEFINE TABLE t;
		FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = $i, k = $i % 7, s = <string> ($i % 13) };
	";
	let config =
		surrealdb_cnf::ConfigMap::empty().with_key_value("external_sorting_buffer_limit", "100");
	let db = TestDb::new_with_temporary_directory(setup, dir.path(), config).await;
	let ctx = db.exec_ctx().await;
	let mut golden = Golden::new("a_spilling_sort_orders_its_rows_ties_included");
	let before = super::spill::RUNS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed);
	for sql in [
		"SELECT * FROM t ORDER BY k TEMPFILES",
		"SELECT n, k FROM t ORDER BY k DESC, s TEMPFILES",
		"SELECT VALUE n FROM t ORDER BY s COLLATE, k TEMPFILES",
		"SELECT n FROM t WHERE n % 3 = 0 ORDER BY k TEMPFILES",
		"SELECT n FROM t ORDER BY k LIMIT 5 START 2000 TEMPFILES",
		"SELECT n FROM t ORDER BY k LIMIT 5 TEMPFILES",
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		golden.rows(sql, &outcome(&vm, &ctx).await);
	}
	assert!(
		super::spill::RUNS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed) > before,
		"no sort spilled"
	);
	golden.check();
}

/// Path steps and `THROW` the VM lowers to instructions answer the owner and a
/// checked record user, over rows that are records and rows that are not.
#[tokio::test]
async fn native_path_steps_and_throws_answer_the_owner_and_a_record_user() {
	let mut golden = Golden::new("native_path_steps_and_throws_answer_the_owner_and_a_record_user");
	let setup = "
		DEFINE TABLE p PERMISSIONS FULL;
		DEFINE FIELD secret ON p PERMISSIONS FOR select WHERE $auth.id = user:x;
		CREATE p:1 SET m = 2, tags = [{ n: 1, k: 'a' }, { n: 5, k: 'b' }, { n: 3, k: 'c' }],
			s = <set> [1, 2, 3], link = p:2, links = [p:2, 7, 'x'], secret = 's1';
		CREATE p:2 SET m = 9, name = 'two', tags = [], secret = 's2';
	";
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	let db = TestDb::new_with_auth(setup).await;
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in [
			"SELECT VALUE a FROM [{ a: 1 }, { a: 2 }, 3, NONE]",
			"SELECT VALUE $this.a FROM [{ a: 1 }, 5]",
			"SELECT VALUE name FROM [p:1, p:2]",
			"SELECT VALUE secret FROM [p:1, p:2]",
			"SELECT link.* AS l FROM p",
			"SELECT VALUE links.* FROM p",
			"SELECT VALUE (NONE).* FROM p",
			"SELECT tags[WHERE n > 2] AS t FROM p",
			"SELECT VALUE tags[WHERE n > $parent.m] FROM p",
			"SELECT VALUE tags[WHERE k = 'b' OR n = 1].k FROM p",
			"SELECT VALUE (<set> [{ n: 1 }, { n: 3 }, { n: 3 }])[WHERE n > 1] FROM p",
			"SELECT VALUE m[WHERE n > 1] FROM p",
			"SELECT VALUE s[WHERE $this > 1] FROM p",
			"SELECT VALUE [p:1, p:2][WHERE secret = 's1'] FROM p:1",
			"SELECT VALUE IF m > 5 THEN THROW 'big ' + <string> m ELSE m END FROM p",
			"SELECT VALUE @.id FROM [p:1, { id: p:2 }, { id: 'fake' }, 3]",
			"SELECT * FROM (BREAK)",
		] {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
		}
	}
	golden.check();
}

/// A table whose images a write shows through its field view: computed
/// fields, field guards, and a computed field reading a guarded one.
pub(super) const VIEWED_SCHEMA: &str = "
	DEFINE TABLE doc PERMISSIONS FULL;
	DEFINE FIELD secret ON doc PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD veiled ON doc PERMISSIONS FOR select WHERE open > 1;
	DEFINE FIELD twice ON doc COMPUTED n * 2;
	DEFINE FIELD echoed ON doc COMPUTED secret PERMISSIONS FOR select WHERE n > 1;
	CREATE doc:1 SET n = 1, secret = 's1', veiled = 'v1', open = 1;
	CREATE doc:2 SET n = 2, secret = 's2', veiled = 'v2', open = 2;
	DEFINE TABLE other PERMISSIONS FULL;
	CREATE other:1 SET n = 1;
";

/// Writes to [`VIEWED_SCHEMA`]'s table under each `RETURN` form.
const VIEWED_WRITES: &[&str] = &[
	"UPDATE doc SET n += 1",
	"UPDATE doc SET n += 1 RETURN AFTER",
	"UPDATE doc SET n += 1 RETURN BEFORE",
	"UPDATE doc SET n += 1 RETURN DIFF",
	"UPDATE doc SET n += 1 RETURN NONE",
	"UPDATE doc SET n += 1 RETURN NULL",
	"UPDATE doc SET n += 1 RETURN *",
	"UPDATE doc SET n += 1 RETURN VALUE twice",
	"UPDATE doc SET n += 1 RETURN twice, secret, $before.secret AS b, $after.echoed AS a",
	"UPDATE doc SET n += 1 RETURN *, n * 10 AS tens",
	"UPDATE doc:1, doc:2 SET open = 5",
	"UPDATE doc:1 SET n = 3 RETURN AFTER",
	"CREATE doc:3 SET n = 5, secret = 'x', open = 3",
	"CREATE doc:3 SET n = 5 RETURN BEFORE",
	"UPSERT doc:9 SET n = 9 RETURN AFTER",
	"INSERT INTO doc { id: doc:7, n: 7, secret: 'i', open: 2 }",
	"DELETE doc:1 RETURN BEFORE",
	"DELETE doc:1 RETURN n, secret",
	"DELETE doc:1",
	"DELETE doc RETURN DIFF",
];

/// Run `sql` against a fresh [`VIEWED_SCHEMA`] as `session` and answer its rows
/// and the table's records after.
pub(super) async fn written_as(
	sql: &str,
	session: &crate::dbs::Session,
) -> (Result<Vec<Value>, String>, Vec<String>) {
	let db = TestDb::new_with_auth(VIEWED_SCHEMA).await;
	let ctx = db.exec_ctx_as(session, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let rows = outcome(&op, &ctx).await;
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let read = db.exec_ctx().await;
	let op = crate::exec::operators::test_util::plan_statement("SELECT * FROM doc", &read).await;
	let mut state: Vec<String> = outcome(&op, &read)
		.await
		.expect("the state reads")
		.into_iter()
		.map(|row| row.to_sql())
		.collect();
	state.sort();
	(rows, state)
}

/// A write shows each image through the table's field view — to its owner and
/// to a record user whose reads are checked — with the output stage run as
/// instructions.
#[tokio::test]
async fn a_write_returns_its_images_through_the_field_view() {
	let mut golden = Golden::new("a_write_returns_its_images_through_the_field_view");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in VIEWED_WRITES {
			let (rows, state) = written_as(sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_written(&rows, &state));
			// The owner is shown the guarded field; the record user never is.
			let shown = format!("{:?}", rows.expect("the write succeeds"));
			if *sql == "UPDATE doc SET n += 1 RETURN AFTER" {
				assert_eq!(shown.contains("s1"), who == "owner", "`{sql}` shows the {who} {shown}");
				assert!(shown.contains("twice"), "`{sql}` shows the {who} {shown}");
			}
			if who != "owner" {
				assert!(!shown.contains("s1") && !shown.contains("s2"), "`{sql}` shows {shown}");
			}
			let db = TestDb::new_with_auth(VIEWED_SCHEMA).await;
			let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Write).await;
			let bound = bind(&ctx, sql).await;
			let physical = super::optimize::optimize(
				super::map::map(&planner(&ctx), &bound).await.expect("maps"),
			);
			let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
			assert!(
				write_families(&with_stages(&planner(&ctx), &program).await).contains(&"output"),
				"`{sql}` for the {who} shows its output by no instruction"
			);
			ctx.txn().cancel().await.expect("cancel");
		}
	}
	golden.check();
}

/// Writes to [`VIEWED_SCHEMA`]'s table whose targets are evaluated or name
/// more than one table, so their stages lower per record's table.
pub(super) const EVALUATED_TARGET_WRITES: &[&str] = &[
	"UPDATE type::table('doc') SET n += 1",
	"UPDATE type::table('doc') SET n += 1 RETURN BEFORE",
	"UPDATE doc:1, other:1 SET n = 5",
	"UPDATE type::record('doc', 2) SET n = 7 RETURN DIFF",
	"CREATE type::record('doc', 5) SET n = 5, secret = 'z' RETURN AFTER",
	"CREATE doc:6, other:6 SET n = 6",
	"UPSERT type::record('doc', 9) SET n = 9",
	"DELETE type::table('doc') RETURN BEFORE",
	"INSERT { id: doc:8, n: 8, secret: 'i' }",
	"UPDATE doc:1, doc:2 SET open = 4 RETURN n, secret",
	"UPDATE (SELECT VALUE id FROM doc) SET open = 4 RETURN n, secret",
];

/// A write whose table is known only as its records arrive carries a unit of
/// stages for each table it can reach, lowered when the statement arrives, and
/// answers the owner and a checked record user.
#[tokio::test]
async fn a_write_to_evaluated_targets_carries_its_stages_per_table() {
	let mut golden = Golden::new("a_write_to_evaluated_targets_carries_its_stages_per_table");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	let mut sited = 0;
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in EVALUATED_TARGET_WRITES {
			let (rows, state) = written_as(sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_written(&rows, &state));
			if who != "owner"
				&& let Ok(rows) = &rows
			{
				let shown = format!("{rows:?}");
				assert!(!shown.contains("s1") && !shown.contains("s2"), "`{sql}` shows {shown}");
			}
			// The units are lowered for the table as a record of it would lower
			// them, with none of the evaluating stages left to their Rust
			// bodies.
			let db = TestDb::new_with_auth(VIEWED_SCHEMA).await;
			let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Write).await;
			let bound = bind(&ctx, sql).await;
			let physical = super::optimize::optimize(
				super::map::map(&planner(&ctx), &bound).await.expect("maps"),
			);
			let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
			let spec = program.write.as_ref().expect("a write");
			// A target binding folds to a literal names its table.
			let Some(tables) = &spec.tables else {
				ctx.txn().cancel().await.expect("cancel");
				continue;
			};
			sited += 1;
			// A write that may reach any table names its stages, and its run
			// loads the bundle of a table's stages by the table's name.
			let stages = match &tables.by_name {
				Some(named) => {
					let (bundle, _) =
						super::lower::stage_bundle_for(&planner(&ctx), named, &"doc".into(), false)
							.await
							.expect("the table's stages lower");
					let entry = bundle.unit.as_ref().and_then(|head| head.entry).expect("a bundle");
					Arc::clone(&bundle.units[entry.0 as usize])
				}
				None => {
					let unit = tables.unit(&"doc".into()).expect("the write reaches the table");
					Arc::clone(&program.units[unit.0 as usize])
				}
			};
			let families = write_families(&stages);
			for family in ["data", "gate", "output", "schema"] {
				assert!(
					families.contains(&family),
					"`{sql}` for the {who} runs no {family} instruction for the table: {families:?}"
				);
			}
			ctx.txn().cancel().await.expect("cancel");
		}
	}
	assert!(sited >= EVALUATED_TARGET_WRITES.len(), "only {sited} writes took a dispatch");
	golden.check();
}

/// Schema statements the VM covers, each run on a fresh database and judged by
/// its answer and by every key and value it leaves.
pub(super) const SCHEMA_STATEMENTS: &[&str] = &[
	"DEFINE NAMESPACE other",
	"DEFINE NAMESPACE other COMMENT 'a namespace'",
	"DEFINE NAMESPACE test",
	"DEFINE NAMESPACE IF NOT EXISTS test",
	"DEFINE NAMESPACE OVERWRITE test COMMENT 'redefined'",
	"DEFINE NAMESPACE OVERWRITE fresh",
	"DEFINE NAMESPACE ('fr' + 'esh')",
	"DEFINE NAMESPACE ('te' + 'st')",
	"DEFINE NAMESPACE (1)",
	"DEFINE DATABASE other",
	"DEFINE DATABASE test",
	"DEFINE DATABASE IF NOT EXISTS test",
	"DEFINE DATABASE OVERWRITE test COMMENT 'again' STRICT",
	"DEFINE DATABASE feed CHANGEFEED 1h INCLUDE ORIGINAL",
	"DEFINE DATABASE ('fr' + 'esh')",
	"DEFINE DATABASE ('te' + 'st')",
	"DEFINE DATABASE (1)",
	"DEFINE TABLE thing",
	"DEFINE TABLE person",
	"DEFINE TABLE IF NOT EXISTS person",
	"DEFINE TABLE OVERWRITE person SCHEMAFULL COMMENT 'people'",
	"DEFINE TABLE thing TYPE NORMAL DROP PERMISSIONS FOR select WHERE $auth.id = id, FOR create NONE",
	"DEFINE TABLE knows TYPE RELATION IN person OUT person ENFORCED",
	"DEFINE TABLE feed CHANGEFEED 3d",
	"DEFINE TABLE capped INLINE EDGES 8",
	"DEFINE FIELD name ON person TYPE string",
	"DEFINE FIELD age ON person TYPE int VALUE $value + 0 ASSERT $value >= 0 DEFAULT 1",
	"DEFINE FIELD tags ON person TYPE array<string>",
	"DEFINE FIELD nested ON person TYPE array<array<int>>",
	"DEFINE FIELD made ON person READONLY DEFAULT ALWAYS time::now() COMMENT 'made'",
	"DEFINE FIELD auto ON newtable TYPE string",
	"DEFINE FIELD in ON knows TYPE record<person>",
	"DEFINE FIELD out ON knows TYPE record<person>",
	"DEFINE FIELD tags[0] ON person TYPE string",
	"DEFINE FIELD tags[*] ON person TYPE string",
	"DEFINE FIELD seen ON person PERMISSIONS FOR select WHERE $auth.id = id",
	"DEFINE FIELD friend ON person TYPE option<record<person>>",
	"DEFINE FIELD existing ON person",
	"DEFINE FIELD IF NOT EXISTS existing ON person",
	"DEFINE FIELD OVERWRITE existing ON person TYPE bool",
	"DEFINE FIELD total ON person COMPUTED 1 + 1",
	"DEFINE PARAM $fresh VALUE 1 + 2",
	"DEFINE PARAM $computed VALUE (SELECT VALUE existing FROM person)",
	"DEFINE PARAM $limit VALUE 20",
	"DEFINE PARAM IF NOT EXISTS $limit VALUE 20",
	"DEFINE PARAM OVERWRITE $limit VALUE 20 COMMENT 'again' PERMISSIONS NONE",
	"DEFINE PARAM $fails VALUE <int> 'x'",
	"DEFINE EVENT fresh ON person WHEN $event = 'CREATE' THEN (CREATE log SET at = time::now())",
	"DEFINE EVENT audit ON person THEN (CREATE log)",
	"DEFINE EVENT IF NOT EXISTS audit ON person THEN (CREATE log)",
	"DEFINE EVENT OVERWRITE audit ON person THEN { CREATE log; CREATE log } COMMENT 'twice'",
	"DEFINE EVENT ('fr' + 'esh') ON person THEN (CREATE log)",
	"DEFINE EVENT ('au' + 'dit') ON person THEN (CREATE log)",
	"DEFINE EVENT (1) ON person THEN (CREATE log)",
	"DEFINE FUNCTION fn::two() { 2 }",
	"DEFINE FUNCTION fn::add($a: int, $b: int) -> int { $a + $b } PERMISSIONS FULL",
	"DEFINE FUNCTION fn::one() { 1 }",
	"DEFINE FUNCTION IF NOT EXISTS fn::one() { 1 }",
	"DEFINE FUNCTION OVERWRITE fn::one() { 11 } COMMENT 'eleven'",
	"DEFINE USER fresh ON ROOT PASSHASH 'hash' ROLES OWNER",
	"DEFINE USER fresh ON NAMESPACE PASSHASH 'hash' ROLES EDITOR DURATION FOR TOKEN 1h, FOR SESSION 2d",
	"DEFINE USER fresh ON DATABASE PASSHASH 'hash' ROLES VIEWER COMMENT 'new'",
	"DEFINE USER root_user ON ROOT PASSHASH 'hash'",
	"DEFINE USER ns_user ON NAMESPACE PASSHASH 'hash'",
	"DEFINE USER db_user ON DATABASE PASSHASH 'hash'",
	"DEFINE USER IF NOT EXISTS db_user ON DATABASE PASSHASH 'hash'",
	"DEFINE USER OVERWRITE db_user ON DATABASE PASSHASH 'other' ROLES EDITOR",
	"DEFINE USER ('fr' + 'esh') ON DATABASE PASSHASH 'hash' ROLES VIEWER",
	"DEFINE USER ('db_' + 'user') ON DATABASE PASSHASH 'hash'",
	"DEFINE USER (1) ON ROOT PASSHASH 'hash'",
	"DEFINE ANALYZER fresh TOKENIZERS blank, class FILTERS lowercase, ascii, snowball(english)",
	"DEFINE ANALYZER words TOKENIZERS blank",
	"DEFINE ANALYZER IF NOT EXISTS words TOKENIZERS blank",
	"DEFINE ANALYZER OVERWRITE unused TOKENIZERS camel COMMENT 'camel'",
	// An access method's evaluated clauses are converted where the body converts them.
	"DEFINE ACCESS fresh ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'se' + 'cret'",
	"DEFINE ACCESS fresh ON DATABASE TYPE JWT ALGORITHM HS256 KEY 1 + 1",
	"DEFINE ACCESS fresh ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'a' + 'b' WITH ISSUER KEY 'a' + 'b'",
	"DEFINE ACCESS fresh ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'a' + 'b' WITH ISSUER KEY 'a' + 'c'",
	"DEFINE ACCESS fresh ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'ab' WITH ISSUER KEY 'a' + 'c'",
	"DEFINE ACCESS fresh ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[RED' + 'ACTED]'",
	"DEFINE ACCESS fresh ON NAMESPACE TYPE JWT URL 'https://' + 'example.com/jwks'",
	"DEFINE ACCESS fresh ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'k' AUDIENCE 'aud' + '1', 'aud2'",
	"DEFINE ACCESS fresh ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM person) DURATION FOR TOKEN 1h + 1h, FOR SESSION 2h + 2h",
	"DEFINE ACCESS fresh ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM person) DURATION FOR TOKEN [NONE][0]",
	"DEFINE ACCESS fresh ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM person) DURATION FOR TOKEN 'x' + 'y'",
	"DEFINE ACCESS fresh ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM person) WITH JWT ALGORITHM HS256 KEY 'k' + 'ey' DURATION FOR TOKEN 1h",
	"DEFINE ACCESS keyed ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 15d + 15d COMMENT 'bear' + 'er'",
	"DEFINE ACCESS ('db_' + 'jwt') ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'se' + 'cret'",
	"DEFINE USER fresh ON DATABASE PASSHASH 'hash' DURATION FOR TOKEN 1h + 1h, FOR SESSION 1d + 1d",
	"DEFINE USER fresh ON DATABASE PASSHASH 'hash' DURATION FOR TOKEN 'x' + 'y'",
	"DEFINE SEQUENCE fresh BATCH 5 * 2 START 3 * 3 TIMEOUT 1s + 1s",
	"DEFINE SEQUENCE fresh BATCH 0 - 1",
	"DEFINE SEQUENCE fresh BATCH -1",
	"DEFINE SEQUENCE fresh START 'x' + 'y'",
	"DEFINE SEQUENCE IF NOT EXISTS seq BATCH 0 - 1",
	"DEFINE CONFIG DEFAULT NAMESPACE 'ot' + 'her' DATABASE 'te' + 'st'",
	"DEFINE CONFIG DEFAULT NAMESPACE [NONE][0]",
	"DEFINE CONFIG DEFAULT NAMESPACE 1 + 1",
	// An API path that is evaluated is parsed where the body parses it.
	"DEFINE API '/fr' + 'esh' FOR get THEN { { status: 200 } }",
	"DEFINE API '/pi' + 'ng' FOR get THEN { { status: 200 } }",
	"DEFINE API 'no' + 'slash' FOR get THEN { { status: 200 } }",
	"DEFINE API '/fresh/' + ':id' FOR get THEN { { status: 200 } } COMMENT 'with ' + 'a param'",
	"ALTER API '/pi' + 'ng' COMMENT 'altered'",
	"ALTER API ('/mis' + 'sing') COMMENT 'x'",
	"REMOVE API '/pi' + 'ng'",
	"REMOVE API '/ping'",
	"REMOVE API IF EXISTS '/mis' + 'sing'",
	"REMOVE API (1)",
	// A comment that is evaluated is cast where the body evaluates it.
	"DEFINE NAMESPACE fresh COMMENT 'a ' + 'namespace'",
	"DEFINE NAMESPACE test COMMENT (1 / 0)",
	"DEFINE NAMESPACE IF NOT EXISTS test COMMENT <string> 1",
	"DEFINE DATABASE fresh COMMENT <string> 1 + 2",
	"DEFINE TABLE fresh COMMENT 'a ' + 'table'",
	"DEFINE TABLE fresh COMMENT 3",
	"DEFINE TABLE ('fr' + 'esh') COMMENT 'evaluated'",
	"DEFINE TABLE (1) COMMENT 'number'",
	"DEFINE PARAM $fresh VALUE 1 COMMENT 'a ' + 'param'",
	"DEFINE EVENT fresh ON person THEN (CREATE log) COMMENT 'an ' + 'event'",
	"DEFINE FUNCTION fn::fresh() { 1 } COMMENT 'a ' + 'function'",
	"DEFINE FUNCTION fn::one() { 1 } COMMENT 'never ' + 'evaluated'",
	"DEFINE USER fresh ON DATABASE PASSHASH 'hash' COMMENT 'a ' + 'user'",
	"DEFINE ANALYZER fresh TOKENIZERS blank COMMENT 'an ' + 'analyzer'",
	"DEFINE ANALYZER fresh TOKENIZERS blank COMMENT [1, 2]",
	"DEFINE API '/fresh' FOR get THEN { { status: 200 } } COMMENT 'an ' + 'api'",
	"DEFINE TABLE light TYPE RELATION LIGHTWEIGHT IN person OUT person",
	"DEFINE TABLE OVERWRITE light TYPE RELATION LIGHTWEIGHT IN person OUT person",
	"DEFINE TABLE rel_change TYPE NORMAL",
	"DEFINE TABLE OVERWRITE rel_change TYPE RELATION IN person OUT person",
	"DEFINE TABLE OVERWRITE rel_change TYPE NORMAL",
	"DEFINE TABLE carried TYPE RELATION IN person OUT person",
	"DEFINE FIELD weight ON carried TYPE int INLINE",
	"DEFINE FIELD OVERWRITE weight ON carried TYPE int INLINE",
	"REMOVE FIELD weight ON carried",
	"{ DEFINE TABLE light TYPE RELATION LIGHTWEIGHT IN person OUT person; DEFINE INDEX fresh ON light FIELDS a }",
	"DEFINE INDEX fresh ON ('per' + 'son') FIELDS existing",
	"DEFINE INDEX fresh ON ('brand' + '_new') FIELDS existing",
	"DEFINE EVENT fresh ON ('per' + 'son') THEN (CREATE log)",
	"DEFINE EVENT fresh ON (1) THEN (CREATE log)",
	// DEFINE TABLE AS: the catalog work in the program, the view filled by its initialiser.
	"DEFINE TABLE fresh_view AS SELECT * FROM person",
	"DEFINE TABLE fresh_view AS SELECT * FROM person WHERE existing = 'a'",
	"DEFINE TABLE counts AS SELECT count() AS n, existing FROM person GROUP BY existing",
	"DEFINE TABLE OVERWRITE by_person AS SELECT existing FROM person",
	"DEFINE TABLE by_person AS SELECT existing FROM person",
	"DEFINE TABLE fresh_view AS SELECT * FROM missing",
	// DEFINE TABLE AS over rows that exist: each kind of view filled from them.
	"DEFINE TABLE sale_copy AS SELECT * FROM sale",
	"DEFINE TABLE sale_big AS SELECT region, amount FROM sale WHERE amount > 5",
	"DEFINE TABLE sale_doubled AS SELECT region, amount * 2 AS doubled FROM sale",
	"DEFINE TABLE sale_unions AS SELECT region FROM sale, person",
	"DEFINE TABLE by_region AS SELECT region, count() AS n, math::sum(amount) AS total FROM sale GROUP BY region",
	"DEFINE TABLE all_sales AS SELECT count() AS n, math::sum(amount) AS total FROM sale GROUP ALL",
	"DEFINE TABLE sale_mean AS SELECT region, math::mean(amount) AS mean FROM sale GROUP BY region",
	"DEFINE TABLE sale_spread AS SELECT region, math::variance(amount) AS var, math::stddev(amount) AS dev FROM sale GROUP BY region",
	"DEFINE TABLE sale_extremes AS SELECT region, math::max(amount) AS hi, math::min(amount) AS lo, time::max(at) AS last, time::min(at) AS first FROM sale GROUP BY region",
	"DEFINE TABLE sale_counted AS SELECT region, count(amount) AS n, count() AS rows FROM sale GROUP BY region",
	"DEFINE TABLE sale_filtered AS SELECT region, count() AS n FROM sale WHERE amount > 5 GROUP BY region",
	"DEFINE TABLE sale_computed AS SELECT region, math::sum(amount * 2) + 1 AS scaled FROM sale GROUP BY region",
	"DEFINE TABLE sale_grouped AS SELECT region, amount > 5 AS big, count() AS n FROM sale GROUP BY region, big",
	"DEFINE TABLE sale_none AS SELECT region, count() AS n FROM sale WHERE amount > 1000 GROUP BY region",
	"DEFINE TABLE sale_none_all AS SELECT count() AS n FROM sale WHERE amount > 1000 GROUP ALL",
	"DEFINE TABLE sale_feed CHANGEFEED 1d AS SELECT region, count() AS n FROM sale GROUP BY region",
	"DEFINE TABLE OVERWRITE by_person AS SELECT existing, id AS of FROM person",
	"DEFINE TABLE from_view AS SELECT existing FROM by_person",
	"DEFINE TABLE from_view_counted AS SELECT count() AS n FROM by_person GROUP ALL",
	"DEFINE TABLE sale_two_sources AS SELECT count() AS n FROM sale, person GROUP ALL",
	// DEFINE FIELD: the body's checks of the definition, made as it makes them.
	"DEFINE FIELD existing ON person COMPUTED 1",
	"DEFINE FIELD id ON person TYPE string",
	"DEFINE FIELD existing.sub ON person TYPE int",
	"DEFINE FIELD loop ON person COMPUTED loop + 1",
	"DEFINE FIELD fresh ON person TYPE int REFERENCE",
	"DEFINE FIELD fresh ON person COMPUTED 1 READONLY",
	"DEFINE FIELD fresh ON person TYPE int GRAPHQL_ALIAS '1bad'",
	"DEFINE FIELD fresh ON person COMPUTED (CREATE log)",
	"DEFINE FIELD nested.deep ON person COMPUTED 1",
	"DEFINE FIELD id ON person VALUE 1",
	"DEFINE FIELD id ON person TYPE bool",
	"DEFINE FIELD fresh ON person TYPE object FLEXIBLE",
	"DEFINE FIELD fresh ON brand_new TYPE object FLEXIBLE",
	"DEFINE FIELD fresh.sub ON person TYPE record REFERENCE",
	"DEFINE FIELD in ON carried TYPE record<sale>",
	"DEFINE FIELD in ON carried TYPE record<sale> COMPUTED 1",
	"DEFINE FIELD OVERWRITE in ON carried TYPE record<person> ASSERT $value != NONE",
	"DEFINE FIELD out ON carried TYPE record<sale> ASSERT $value != NONE",
	// ... against the catalog the block's schema statements before it leave,
	// or, where a condition on data decides one, as the block reaches it.
	"{ DEFINE FIELD obj.sub ON person TYPE int; DEFINE FIELD obj ON person COMPUTED 1 }",
	"{ DEFINE FIELD calc ON person COMPUTED 1; DEFINE FIELD calc.sub ON person TYPE int }",
	"{ DEFINE FIELD a ON person COMPUTED b; DEFINE FIELD b ON person COMPUTED a }",
	"{ DEFINE INDEX by_calc ON person FIELDS calc; DEFINE FIELD calc ON person COMPUTED 1 }",
	"{ IF (SELECT VALUE existing FROM ONLY person:1) = 'a' { DEFINE INDEX by_calc ON person FIELDS calc }; DEFINE FIELD calc ON person COMPUTED 1 }",
	// DEFINE INDEX: the catalog work in the program, the build the builder's.
	"DEFINE INDEX fresh ON person FIELDS existing",
	"DEFINE INDEX fresh ON person FIELDS existing UNIQUE",
	"DEFINE INDEX by_existing ON person FIELDS existing",
	"DEFINE INDEX IF NOT EXISTS by_existing ON person FIELDS existing",
	"DEFINE INDEX OVERWRITE by_existing ON person FIELDS existing UNIQUE",
	"DEFINE INDEX OVERWRITE by_b ON indexed FIELDS b",
	"DEFINE INDEX OVERWRITE by_b ON indexed COUNT",
	"DEFINE INDEX counted ON indexed COUNT",
	"DEFINE INDEX fresh ON person FIELDS nested.x, other",
	"DEFINE INDEX fresh ON person FIELDS existing FULLTEXT ANALYZER words BM25",
	"DEFINE INDEX ('fr' + 'esh') ON person FIELDS existing COMMENT 'a ' + 'comment'",
	"DEFINE INDEX fresh ON indexed FIELDS a, b UNIQUE",
	"DEFINE INDEX fresh ON brand_new FIELDS a",
	"DEFINE INDEX fresh ON brand_new COUNT",
	"DEFINE EVENT fresh ON brand_new THEN (CREATE log)",
	"DEFINE FIELD fresh ON brand_new TYPE int",
	"{ DEFINE DATABASE OVERWRITE test STRICT; DEFINE INDEX fresh ON brand_new FIELDS a }",
	// A name that is evaluated is read as a string where the body resolves it.
	"DEFINE ANALYZER ('fr' + 'esh') TOKENIZERS blank",
	"DEFINE ANALYZER ('wo' + 'rds') TOKENIZERS blank",
	"DEFINE ANALYZER (1) TOKENIZERS blank",
	"REMOVE ANALYZER ('un' + 'used')",
	"REMOVE ANALYZER ('wo' + 'rds')",
	"REMOVE ANALYZER IF EXISTS ('no' + 'ne')",
	"ALTER ANALYZER ('un' + 'used') COMMENT 'altered'",
	"ALTER ANALYZER (1) COMMENT 'altered'",
	"DEFINE SEQUENCE fresh",
	"DEFINE SEQUENCE fresh BATCH 50 START 100 TIMEOUT 5s",
	"DEFINE SEQUENCE seq",
	"DEFINE SEQUENCE IF NOT EXISTS seq",
	"DEFINE SEQUENCE OVERWRITE seq BATCH 10 START 7",
	"DEFINE SEQUENCE ('fr' + 'esh') BATCH 5",
	"DEFINE SEQUENCE ('s' + 'eq')",
	"DEFINE SEQUENCE (1)",
	"DEFINE API \"/fresh\" FOR get, post THEN { { status: 201 } } COMMENT 'made'",
	"DEFINE API \"/fresh/:id\" FOR get MIDDLEWARE api::timeout(2s) PERMISSIONS FULL THEN { { status: 200 } }",
	"DEFINE API \"/ping\" FOR get THEN { { status: 200 } }",
	"DEFINE API IF NOT EXISTS \"/ping\" FOR get THEN { { status: 200 } }",
	"DEFINE API OVERWRITE \"/ping\" FOR any THEN { { status: 204 } }",
	"DEFINE CONFIG GRAPHQL AUTO",
	"DEFINE CONFIG GRAPHQL TABLES INCLUDE person FUNCTIONS NONE",
	"DEFINE CONFIG API MIDDLEWARE api::timeout(5s)",
	"DEFINE CONFIG IF NOT EXISTS API MIDDLEWARE api::timeout(5s)",
	"DEFINE CONFIG OVERWRITE API MIDDLEWARE api::timeout(5s)",
	"DEFINE CONFIG DEFAULT NAMESPACE test DATABASE test",
	"DEFINE CONFIG DEFAULT NAMESPACE 'other'",
	"DEFINE ACCESS fresh ON ROOT TYPE JWT ALGORITHM HS256 KEY 'k'",
	"DEFINE ACCESS fresh ON NAMESPACE TYPE JWT URL 'https://example.com/jwks' DURATION FOR SESSION 2h",
	"DEFINE ACCESS fresh ON DATABASE TYPE RECORD SIGNUP (CREATE person SET existing = $e) SIGNIN (SELECT * FROM person WHERE existing = $e) DURATION FOR TOKEN 15m",
	"DEFINE ACCESS keyed ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 30d COMMENT 'bearer'",
	"DEFINE ACCESS db_jwt ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret'",
	"DEFINE ACCESS IF NOT EXISTS db_jwt ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret'",
	"DEFINE ACCESS OVERWRITE ns_jwt ON NAMESPACE TYPE JWT ALGORITHM HS384 KEY 'other'",
	"DEFINE ACCESS root_jwt ON ROOT TYPE JWT ALGORITHM HS512 KEY 'secret'",
	// ES512 is refused only where the statement goes on to write.
	"DEFINE ACCESS IF NOT EXISTS root_jwt ON ROOT TYPE JWT ALGORITHM ES512 KEY 'k'",
	"DEFINE ACCESS IF NOT EXISTS ns_jwt ON NAMESPACE TYPE JWT ALGORITHM ES512 KEY 'k'",
	"DEFINE ACCESS IF NOT EXISTS db_jwt ON DATABASE TYPE JWT ALGORITHM ES512 KEY 'k'",
	"DEFINE ACCESS IF NOT EXISTS fresh ON DATABASE TYPE JWT ALGORITHM ES512 KEY 'k'",
	"DEFINE ACCESS db_jwt ON DATABASE TYPE JWT ALGORITHM ES512 KEY 'k'",
	"DEFINE ACCESS OVERWRITE db_jwt ON DATABASE TYPE JWT ALGORITHM ES512 KEY 'k'",
	"DEFINE ACCESS ('fr' + 'esh') ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'k'",
	"DEFINE ACCESS ('db_' + 'jwt') ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret'",
	"DEFINE ACCESS (1) ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY 'k'",
	"ALTER TABLE person SCHEMAFULL",
	"ALTER TABLE person SCHEMALESS COMMENT 'people'",
	"ALTER TABLE person PERMISSIONS FOR select FULL, FOR create NONE",
	"ALTER TABLE person CHANGEFEED 1h",
	"ALTER TABLE feedful CHANGEFEED 2h INCLUDE ORIGINAL",
	"ALTER TABLE feedful DROP CHANGEFEED",
	"ALTER TABLE person DROP COMMENT",
	"ALTER TABLE person INLINE EDGES 4",
	"ALTER TABLE person TYPE RELATION IN person OUT person",
	"ALTER TABLE person TYPE NORMAL",
	"ALTER TABLE ('per' + 'son') COMMENT 'altered evaluated'",
	"ALTER TABLE (1) COMMENT 'altered number'",
	"ALTER TABLE missing SCHEMAFULL",
	"ALTER TABLE IF EXISTS missing SCHEMAFULL",
	"ALTER FIELD existing ON person TYPE int",
	"ALTER FIELD existing ON person DROP TYPE READONLY COMMENT 'altered'",
	"ALTER FIELD existing ON person VALUE $value + 1 ASSERT $value > 0 DEFAULT 1",
	"ALTER FIELD existing ON person DEFAULT ALWAYS 2 PERMISSIONS FOR select NONE",
	"ALTER FIELD existing ON person DROP VALUE DROP ASSERT DROP DEFAULT DROP READONLY DROP COMMENT",
	"ALTER FIELD weight ON carried INLINE",
	"ALTER FIELD weight ON carried DROP INLINE",
	"ALTER FIELD missing ON person TYPE int",
	"ALTER FIELD IF EXISTS missing ON person TYPE int",
	"ALTER FIELD id ON person DEFAULT rand::uuid::v7()",
	// A field path, table or comment that is evaluated is resolved where the body resolves it.
	"DEFINE FIELD ('fr' + 'esh') ON person TYPE int",
	"DEFINE FIELD ('fr' + 'esh') ON ('per' + 'son') TYPE int COMMENT 'a ' + 'field'",
	"DEFINE FIELD nested.deep ON ('per' + 'son') TYPE int",
	"DEFINE FIELD tags[$i] ON person TYPE string",
	"DEFINE FIELD (1) ON person TYPE int",
	"DEFINE FIELD ('a' + 'b') ON (1) TYPE int",
	"DEFINE FIELD ('b' + 'c') ON newtable TYPE int",
	"DEFINE FIELD ('exi' + 'sting') ON person TYPE int",
	"DEFINE FIELD IF NOT EXISTS ('exi' + 'sting') ON person TYPE int",
	"DEFINE FIELD OVERWRITE ('exi' + 'sting') ON person TYPE bool",
	"DEFINE FIELD ('ex' + 'isting') ON person COMPUTED 1",
	"REMOVE FIELD ('exi' + 'sting') ON person",
	"REMOVE FIELD existing ON ('per' + 'son')",
	"REMOVE FIELD IF EXISTS ('mis' + 'sing') ON person",
	"REMOVE FIELD ('mis' + 'sing') ON person",
	"REMOVE FIELD (1) ON person",
	"ALTER FIELD ('exi' + 'sting') ON person TYPE int",
	"ALTER FIELD existing ON ('per' + 'son') DROP TYPE COMMENT 'altered'",
	"ALTER FIELD IF EXISTS ('mis' + 'sing') ON person TYPE int",
	"ALTER FIELD ('mis' + 'sing') ON person TYPE int",
	"ALTER FIELD (1) ON person TYPE int",
	// An index column that is evaluated is resolved where the body resolves it.
	"DEFINE INDEX evaluated_ix ON person FIELDS ('na' + 'me')",
	"DEFINE INDEX evaluated_ix ON person FIELDS name, ('ag' + 'e')",
	"DEFINE INDEX evaluated_ix ON person FIELDS (1)",
	"DEFINE INDEX evaluated_ix ON person FIELDS ('fr' + 'esh') UNIQUE",
	"DEFINE INDEX evaluated_ix ON person FIELDS ('to' + 'tal')",
	// A refusing API path or a method given twice is raised where the body raises it.
	"DEFINE API 'no slash' FOR get THEN { { status: 200 } }",
	"DEFINE API '/fresh' FOR get THEN { { status: 200 } } FOR get THEN { { status: 201 } }",
	"DEFINE API 'no slash' FOR get THEN { { status: 200 } } FOR get THEN { { status: 201 } }",
	"DEFINE API '/fresh' FOR get, post THEN { { status: 200 } } FOR post THEN { { status: 201 } }",
	"DEFINE API IF NOT EXISTS '/ping' FOR get THEN { { status: 200 } } FOR get THEN { { status: 201 } }",
	// A middleware argument that is evaluated is evaluated where the body evaluates it.
	"DEFINE API '/fresh' FOR get MIDDLEWARE api::timeout(1s + 1s) THEN { { status: 200 } }",
	"DEFINE API '/fresh' FOR any MIDDLEWARE api::timeout(1s + 1s) THEN { { status: 200 } }",
	"DEFINE API '/fresh' FOR get MIDDLEWARE api::timeout(1s + 1s) THEN { { status: 200 } } COMMENT 'with ' + 'a middleware'",
	"DEFINE CONFIG API MIDDLEWARE api::timeout(1s + 1s)",
	"DEFINE CONFIG OVERWRITE API MIDDLEWARE api::timeout(1s + 1s)",
	"ALTER API '/ping' FOR get MIDDLEWARE api::timeout(1s + 1s) THEN { { status: 201 } }",
	"ALTER API '/ping' FOR any MIDDLEWARE api::timeout(1s + 1s) THEN { { status: 201 } }",
	// An access name that is evaluated is read where the body reads it.
	"ALTER ACCESS ('db_' + 'jwt') ON DATABASE COMMENT 'altered'",
	"ALTER ACCESS ('mis' + 'sing') ON DATABASE COMMENT 'altered'",
	"ALTER ACCESS IF EXISTS ('mis' + 'sing') ON DATABASE COMMENT 'altered'",
	"ALTER ACCESS (1) ON DATABASE COMMENT 'altered'",
	"ALTER ACCESS ('root_' + 'jwt') ON ROOT COMMENT 'altered'",
	"ALTER ACCESS db_jwt ON DATABASE DURATION FOR TOKEN 1h",
	// A type change is validated against the table as the program finds it.
	"ALTER TABLE ('per' + 'son') TYPE RELATION IN person OUT person",
	"ALTER TABLE missing TYPE RELATION IN person OUT person",
	"ALTER TABLE IF EXISTS missing TYPE RELATION IN person OUT person",
	// ALTER FIELD: the body's checks of the definition the clauses merge into.
	"ALTER FIELD id ON keyed_by READONLY",
	"ALTER FIELD id ON keyed_by FLEXIBLE",
	"ALTER FIELD id ON keyed_by VALUE 'x'",
	"ALTER FIELD id ON keyed_by DEFAULT ALWAYS 'x'",
	"ALTER FIELD id ON keyed_by TYPE bool",
	"ALTER FIELD id ON keyed_by TYPE uuid DEFAULT rand::uuid() ASSERT $value != NONE COMMENT 'key'",
	"ALTER FIELD existing ON person FLEXIBLE",
	"ALTER FIELD loose ON person FLEXIBLE",
	"ALTER FIELD doc ON strict FLEXIBLE",
	"ALTER FIELD doc ON strict FLEXIBLE TYPE string",
	"ALTER FIELD doc ON strict DROP FLEXIBLE TYPE string",
	"ALTER NAMESPACE COMPACT",
	"ALTER DATABASE COMPACT",
	"ALTER TABLE person COMPACT",
	"ALTER PARAM $limit VALUE 30 + 1 COMMENT 'thirty-one'",
	"ALTER PARAM $limit DROP COMMENT PERMISSIONS NONE",
	"ALTER PARAM $missing VALUE 1",
	"ALTER PARAM IF EXISTS $missing VALUE 1",
	"ALTER FUNCTION fn::one($x: int) -> int { $x } COMMENT 'identity'",
	"ALTER FUNCTION fn::one DROP COMMENT PERMISSIONS NONE",
	"ALTER FUNCTION fn::missing COMMENT 'x'",
	"ALTER FUNCTION IF EXISTS fn::missing COMMENT 'x'",
	"ALTER EVENT audit ON person WHEN $event = 'DELETE' THEN (CREATE log) COMMENT 'deletes'",
	"ALTER EVENT audit ON person DROP COMMENT",
	"ALTER EVENT missing ON person COMMENT 'x'",
	"ALTER EVENT IF EXISTS missing ON person COMMENT 'x'",
	"ALTER EVENT ('au' + 'dit') ON ('per' + 'son') COMMENT 'x'",
	"ALTER EVENT audit ON (1) COMMENT 'x'",
	"ALTER EVENT (1) ON (2) COMMENT 'x'",
	"ALTER SEQUENCE seq TIMEOUT 3s",
	"ALTER SEQUENCE missing TIMEOUT 3s",
	"ALTER SEQUENCE IF EXISTS missing TIMEOUT 3s",
	"ALTER SEQUENCE ('s' + 'eq') TIMEOUT 3s",
	"ALTER SEQUENCE (1) TIMEOUT 3s",
	"ALTER ANALYZER unused TOKENIZERS blank FILTERS uppercase COMMENT 'upper'",
	"ALTER ANALYZER unused DROP FILTERS DROP COMMENT",
	"ALTER ANALYZER missing COMMENT 'x'",
	"ALTER ANALYZER IF EXISTS missing COMMENT 'x'",
	"ALTER USER db_user ON DATABASE ROLES EDITOR COMMENT 'edits'",
	"ALTER USER ns_user ON NAMESPACE PASSHASH 'new' DURATION FOR TOKEN 5m, FOR SESSION NONE",
	"ALTER USER root_user ON ROOT DROP COMMENT",
	"ALTER USER missing ON DATABASE ROLES EDITOR",
	"ALTER USER IF EXISTS missing ON NAMESPACE ROLES EDITOR",
	"ALTER USER ('db_' + 'user') ON DATABASE ROLES EDITOR",
	"ALTER USER ('mis' + 'sing') ON ROOT ROLES EDITOR",
	"ALTER CONFIG API MIDDLEWARE api::timeout(9s)",
	"ALTER CONFIG IF EXISTS GRAPHQL AUTO",
	"ALTER CONFIG GRAPHQL AUTO",
	"ALTER INDEX by_a ON indexed COMMENT 'by a'",
	"ALTER INDEX by_a ON indexed PREPARE REMOVE",
	"ALTER INDEX missing ON indexed COMMENT 'x'",
	"ALTER INDEX IF EXISTS missing ON indexed COMMENT 'x'",
	"ALTER INDEX ('by_' + 'a') ON ('index' + 'ed') COMMENT 'by a'",
	"ALTER INDEX (1) ON indexed COMMENT 'x'",
	"ALTER INDEX by_a ON (1) COMMENT 'x'",
	"ALTER API \"/ping\" COMMENT 'pinged'",
	"ALTER API \"/ping\" FOR any THEN { RETURN 'fallback' }",
	"ALTER API \"/ping\" FOR any DROP THEN",
	"ALTER API \"/ping\" FOR get DROP THEN",
	"ALTER API \"/ping\" FOR post THEN { RETURN 'post' } FOR get DROP THEN FOR put THEN { RETURN 'put' }",
	"ALTER API \"/missing\" COMMENT 'x'",
	"ALTER API IF EXISTS \"/missing\" COMMENT 'x'",
	"ALTER ACCESS db_jwt ON DATABASE DURATION FOR TOKEN 2h, FOR SESSION 48h COMMENT 'updated'",
	"ALTER ACCESS ns_jwt ON NAMESPACE DURATION FOR TOKEN NONE DROP COMMENT",
	"ALTER ACCESS root_jwt ON ROOT AUTHENTICATE { $auth }",
	"ALTER ACCESS missing ON DATABASE COMMENT 'x'",
	"ALTER ACCESS IF EXISTS missing ON ROOT COMMENT 'x'",
	"ALTER SYSTEM COMPACT",
	"ALTER SYSTEM QUERY_TIMEOUT 5s",
	"ALTER SYSTEM DROP QUERY_TIMEOUT",
	"INFO FOR DB",
	"INFO FOR DB STRUCTURE",
	"INFO FOR TABLE person",
	"INFO FOR TABLE indexed STRUCTURE",
	"INFO FOR TABLE 'person'",
	"INFO FOR TABLE missing",
	"INFO FOR NS",
	"INFO FOR NS STRUCTURE",
	"INFO FOR ROOT",
	"INFO FOR ROOT STRUCTURE",
	"INFO FOR USER db_user ON DATABASE",
	"INFO FOR USER ns_user ON NAMESPACE STRUCTURE",
	"INFO FOR USER root_user ON ROOT",
	"INFO FOR USER missing ON DATABASE",
	// At the base the session selects, read when the statement runs.
	"INFO FOR USER db_user",
	"INFO FOR USER db_user STRUCTURE",
	"INFO FOR USER ns_user",
	"INFO FOR USER ('db' + '_user')",
	"INFO FOR INDEX by_a ON indexed",
	"INFO FOR INDEX by_a ON indexed STRUCTURE",
	"INFO FOR INDEX ('by_' + 'a') ON ('index' + 'ed')",
	"INFO FOR INDEX missing ON indexed",
	"REMOVE CONFIG IF EXISTS GRAPHQL",
	"REMOVE CONFIG GRAPHQL",
	"REMOVE CONFIG API",
	"REMOVE CONFIG IF EXISTS DEFAULT",
	"REMOVE NAMESPACE test",
	"REMOVE NAMESPACE AND EXPUNGE test",
	"REMOVE NAMESPACE missing",
	"REMOVE NAMESPACE IF EXISTS missing",
	"REMOVE NAMESPACE ('te' + 'st')",
	"REMOVE NAMESPACE (1)",
	"REMOVE DATABASE test",
	"REMOVE DATABASE AND EXPUNGE second",
	"REMOVE DATABASE missing",
	"REMOVE DATABASE IF EXISTS missing",
	"REMOVE DATABASE ('te' + 'st')",
	"REMOVE DATABASE IF EXISTS ('mis' + 'sing')",
	"REMOVE TABLE person",
	"REMOVE TABLE by_person",
	"REMOVE TABLE IF EXISTS by_person",
	"REMOVE TABLE indexed",
	"REMOVE TABLE AND EXPUNGE indexed",
	"REMOVE TABLE missing",
	"REMOVE TABLE IF EXISTS missing",
	"REMOVE TABLE ('in' + 'dexed')",
	"REMOVE TABLE IF EXISTS ('mis' + 'sing')",
	"REMOVE TABLE (1)",
	"REMOVE EVENT audit ON person",
	"REMOVE EVENT missing ON person",
	"REMOVE EVENT IF EXISTS missing ON person",
	"REMOVE EVENT ('au' + 'dit') ON ('per' + 'son')",
	"REMOVE EVENT audit ON (1)",
	"REMOVE FIELD existing ON person",
	"REMOVE FIELD missing ON person",
	"REMOVE FIELD IF EXISTS missing ON person",
	"REMOVE INDEX by_a ON indexed",
	"REMOVE INDEX ('by_' + 'a') ON ('index' + 'ed')",
	"REMOVE INDEX (1) ON indexed",
	"REMOVE INDEX by_b ON indexed",
	"REMOVE INDEX only_text ON texts",
	"REMOVE INDEX missing ON indexed",
	"REMOVE INDEX IF EXISTS missing ON indexed",
	"REMOVE PARAM $limit",
	"REMOVE PARAM $missing",
	"REMOVE PARAM IF EXISTS $missing",
	"REMOVE FUNCTION fn::one",
	"REMOVE FUNCTION fn::missing",
	"REMOVE FUNCTION IF EXISTS fn::missing",
	"REMOVE SEQUENCE seq",
	"REMOVE SEQUENCE missing",
	"REMOVE SEQUENCE ('s' + 'eq')",
	"REMOVE SEQUENCE IF EXISTS ('mis' + 'sing')",
	"REMOVE USER ('db_' + 'user') ON DATABASE",
	"REMOVE USER ('ns_' + 'user') ON NAMESPACE",
	"REMOVE USER (1) ON ROOT",
	"REMOVE ACCESS ('db_' + 'jwt') ON DATABASE",
	"REMOVE ACCESS ('mis' + 'sing') ON ROOT",
	"REMOVE SEQUENCE IF EXISTS missing",
	"REMOVE ANALYZER words",
	"REMOVE ANALYZER unused",
	"REMOVE ANALYZER missing",
	"REMOVE ANALYZER IF EXISTS missing",
	"REMOVE ACCESS root_jwt ON ROOT",
	"REMOVE ACCESS ns_jwt ON NAMESPACE",
	"REMOVE ACCESS db_jwt ON DATABASE",
	"REMOVE ACCESS missing ON ROOT",
	"REMOVE ACCESS missing ON NAMESPACE",
	"REMOVE ACCESS missing ON DATABASE",
	"REMOVE ACCESS IF EXISTS missing ON DATABASE",
	"REMOVE API \"/ping\"",
	"REMOVE API \"/missing\"",
	"REMOVE API IF EXISTS \"/missing\"",
	"REMOVE TABLE 'indexed'",
	"REMOVE USER root_user ON ROOT",
	"REMOVE USER ns_user ON NAMESPACE",
	"REMOVE USER db_user ON DATABASE",
	"REMOVE USER missing ON ROOT",
	"REMOVE USER missing ON NAMESPACE",
	"REMOVE USER missing ON DATABASE",
	"REMOVE USER IF EXISTS missing ON NAMESPACE",
];

/// The database [`defined`] runs a statement against.
pub(super) const DEFINED_SCHEMA: &str = "DEFINE TABLE person; DEFINE FIELD existing ON person TYPE string; \
		 DEFINE CONFIG API MIDDLEWARE api::timeout(1s); \
		 DEFINE INDEX by_existing ON person FIELDS existing; DEFINE SEQUENCE seq; \
		 CREATE person:1 SET existing = 'a'; DEFINE TABLE by_person AS SELECT * FROM person; \
		 CREATE sale:1 SET region = 'eu', amount = 10, at = d'2024-01-01T00:00:00Z'; \
		 CREATE sale:2 SET region = 'eu', amount = 20, at = d'2024-03-01T00:00:00Z'; \
		 CREATE sale:3 SET region = 'us', amount = 5, at = d'2024-02-01T00:00:00Z'; \
		 CREATE sale:4 SET region = 'us', amount = 7; \
		 DEFINE TABLE indexed; DEFINE INDEX by_a ON indexed FIELDS a; CREATE indexed:1 SET a = 1; \
		 DEFINE TABLE carried TYPE RELATION IN person OUT person; DEFINE FIELD weight ON carried TYPE int; \
		 DEFINE TABLE feedful CHANGEFEED 1d; \
		 DEFINE FIELD id ON keyed_by TYPE string; DEFINE FIELD loose ON person TYPE object; \
		 DEFINE TABLE strict SCHEMAFULL; DEFINE FIELD doc ON strict TYPE object; \
		 DEFINE EVENT audit ON person THEN (CREATE log); DEFINE ANALYZER words TOKENIZERS blank; \
		 DEFINE INDEX by_b ON indexed FIELDS b FULLTEXT ANALYZER words BM25; \
		 DEFINE INDEX by_c ON indexed FIELDS c FULLTEXT ANALYZER words BM25; \
		 DEFINE INDEX only_text ON texts FIELDS t FULLTEXT ANALYZER words BM25; \
		 CREATE texts:1 SET t = 'some words'; RETURN sequence::nextval('seq'); \
		 DEFINE PARAM $limit VALUE 10; DEFINE FUNCTION fn::one() { 1 }; \
		 DEFINE ANALYZER unused TOKENIZERS blank FILTERS lowercase; \
		 DEFINE ACCESS root_jwt ON ROOT TYPE JWT ALGORITHM HS512 KEY 'secret'; \
		 DEFINE ACCESS ns_jwt ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY 'secret'; \
		 DEFINE ACCESS db_jwt ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret'; \
		 DEFINE API \"/ping\" FOR get THEN { { status: 200 } }; \
		 DEFINE USER root_user ON ROOT PASSWORD 'pass' ROLES VIEWER; \
		 DEFINE USER ns_user ON NAMESPACE PASSWORD 'pass' ROLES VIEWER; \
		 DEFINE USER db_user ON DATABASE PASSWORD 'pass' ROLES VIEWER; \
		 DEFINE DATABASE second; USE DB second; DEFINE TABLE t; DEFINE INDEX ix ON t FIELDS a; \
		 CREATE t:1 SET a = 1";

/// Run schema statement `sql` on a fresh database and answer its result and
/// the keyspace entries it changes.
///
/// Lowering `DEFINE USER` draws the user's random code, which the entries are
/// compared without.
pub(super) async fn defined(
	(sql, parsed): (&str, &LogicalOp),
) -> (Result<Vec<Value>, String>, Vec<(Vec<u8>, Option<Vec<u8>>)>) {
	let db = TestDb::quiescent(DEFINED_SCHEMA, false).await;
	let before = keyspace(&db).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind_parsed(&ctx, parsed.clone()).await;
	// Planned as the executor plans it: against what the statement's own
	// schema statements write.
	let op = planned(&planner(&ctx), &bound, sql).await;
	let rows = outcome(&op, &ctx).await;
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let after = keyspace(&db).await;
	let node = *db.datastore().id().as_bytes();
	let mut changed: Vec<_> = changes(&before, &after)
		.into_iter()
		.map(|(key, value)| {
			let value = value.map(|v| without_node(&without_drawn_secrets(&key, &v), &node));
			(without_node(&key, &node), value)
		})
		.collect();
	changed.sort();
	(rows, changed)
}

/// An access definition without the signing key drawn for it at random when
/// its statement is parsed: every run of 100 or more alphanumeric bytes reads
/// as the same bytes.
fn without_drawn_keys(value: &[u8]) -> Vec<u8> {
	let mut out = value.to_vec();
	let mut at = 0;
	while at < out.len() {
		if !out[at].is_ascii_alphanumeric() {
			at += 1;
			continue;
		}
		let mut end = at;
		while end < out.len() && out[end].is_ascii_alphanumeric() {
			end += 1;
		}
		if end - at >= 100 {
			out[at..end].fill(b'x');
		}
		at = end;
	}
	out
}

/// A user definition without what is drawn at random when the user is
/// defined in each datastore's setup: its code, and the hash and SCRAM
/// verifier salted from a `PASSWORD`. A `PASSHASH` is kept.
fn without_drawn_secrets(key: &[u8], value: &[u8]) -> Vec<u8> {
	if key.windows(3).any(|w| w == b"!ac") {
		return without_drawn_keys(value);
	}
	if !key.windows(3).any(|w| w == b"!us") {
		return value.to_vec();
	}
	let Ok(Value::Object(mut user)) = super::codec::decode("UserDefinition", value) else {
		return value.to_vec();
	};
	user.insert("code".to_owned(), Value::String("".into()));
	if matches!(user.get("hash"), Some(Value::String(h)) if h.as_str().starts_with("$argon2")) {
		user.insert("hash".to_owned(), Value::String("".into()));
		user.insert("scram".to_owned(), Value::None);
	}
	super::codec::encode("UserDefinition", &Value::Object(user)).unwrap_or_else(|_| value.to_vec())
}

/// `bytes` with the datastore's node id zeroed: sequence batches and states,
/// and full-text doc-length entries, embed the id of the node that wrote them,
/// which differs between datastores.
fn without_node(bytes: &[u8], node: &[u8; 16]) -> Vec<u8> {
	let mut out = bytes.to_vec();
	let mut i = 0;
	while i + 16 <= out.len() {
		if out[i..i + 16] == *node {
			out[i..i + 16].fill(0);
			i += 16;
		} else {
			i += 1;
		}
	}
	out
}

/// A change feed row's key without its commit versionstamp:
/// `/*{ns}*{db}#{ts}*{tb}`, stamped — at an encoded length that varies — when
/// the transaction commits.
pub(super) fn without_versionstamp(key: &[u8]) -> Vec<u8> {
	let mut out = key.to_vec();
	if out.len() > 12
		&& out[..2] == *b"/*"
		&& out[6] == b'*'
		&& out[11] == b'#'
		&& let Some(end) = out.iter().rposition(|b| *b == b'*').filter(|end| *end > 12)
	{
		out.drain(12..end);
	}
	out
}

/// `bytes` with every version-7 uuid minted in the last day zeroed: a cache
/// stamp or catalog version differs between any two runs.
pub(super) fn without_fresh_uuids(bytes: &[u8]) -> Vec<u8> {
	let now_ms = web_time::SystemTime::now()
		.duration_since(web_time::UNIX_EPOCH)
		.map(|d| d.as_millis() as u64)
		.unwrap_or(0);
	let mut out = bytes.to_vec();
	let mut i = 0;
	while i + 16 <= out.len() {
		let w = &out[i..i + 16];
		let ms = u64::from_be_bytes([0, 0, w[0], w[1], w[2], w[3], w[4], w[5]]);
		if w[6] >> 4 == 7 && w[8] >> 6 == 2 && now_ms.abs_diff(ms) < 86_400_000 {
			out[i..i + 16].fill(0);
			i += 16;
		} else {
			i += 1;
		}
	}
	out
}

pub(super) async fn keyspace(db: &TestDb) -> Vec<(Vec<u8>, Vec<u8>)> {
	let read = db
		.datastore()
		.transaction(crate::kvs::TransactionType::Read)
		.await
		.expect("a read transaction opens");
	let all = read
		.getr_raw(surrealdb_kvs::key::RawRange::every_key(), None)
		.await
		.expect("the keyspace reads");
	read.cancel().await.expect("the read transaction ends");
	all
}

/// A record key and its value with the id drawn for the record at random
/// replaced by a fixed one: the record's key carries it as a 20-character
/// string, and the record's value holds it again as its `id`.
fn without_generated_id(key: &[u8], value: Option<Vec<u8>>) -> (Vec<u8>, Option<Vec<u8>>) {
	const LENGTH: usize = 20;
	let drawn = key.windows(LENGTH + 2).find(|w| {
		w[0] == 3
			&& w[LENGTH + 1] == 0
			&& w[1..=LENGTH].iter().all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
	});
	let Some(window) = drawn else {
		return (key.to_vec(), value);
	};
	let id = window[1..=LENGTH].to_vec();
	let replace = |bytes: &[u8]| -> Vec<u8> {
		let mut out = bytes.to_vec();
		let mut i = 0;
		while i + LENGTH <= out.len() {
			if out[i..i + LENGTH] == id[..] {
				out[i..i + LENGTH].fill(b'r');
				i += LENGTH;
			} else {
				i += 1;
			}
		}
		out
	};
	(replace(key), value.map(|v| replace(&v)))
}

/// The entries a statement added, changed or removed, without what differs
/// between any two runs: fresh version-7 uuids, change feed versionstamps and
/// the ids drawn for records created with none.
pub(super) fn changes(
	before: &[(Vec<u8>, Vec<u8>)],
	after: &[(Vec<u8>, Vec<u8>)],
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
	let mask = |key: &[u8], value: Option<&Vec<u8>>| -> (Vec<u8>, Option<Vec<u8>>) {
		// An index build's state holds the clock times its builder read, which
		// differ between any two runs; that it is written is what compares.
		let build_state = key.windows(4).any(|w| w == b"!bs\x00");
		// A catalog version carries the print of the definition it was
		// stamped beside, a digest of bytes that hold fresh uuids; the
		// revision and the version before it are what compares.
		let version = key.len() > 14 && matches!(&key[11..14], b"!cv" | b"!ov");
		let (key, value) = without_generated_id(
			key,
			value.map(|v| {
				if build_state {
					Vec::new()
				} else if version {
					without_fresh_uuids(&v[..v.len().min(17)])
				} else {
					without_fresh_uuids(v)
				}
			}),
		);
		(without_versionstamp(&without_fresh_uuids(&key)), value)
	};
	let mut out = Vec::new();
	for (key, value) in after {
		match before.iter().find(|(k, _)| k == key) {
			Some((_, old)) if old == value => {}
			_ => out.push(mask(key, Some(value))),
		}
	}
	for (key, _) in before {
		if !after.iter().any(|(k, _)| k == key) {
			out.push(mask(key, None));
		}
	}
	out.sort();
	out.dedup();
	out
}

pub(super) type Outcome = (Result<Vec<Value>, String>, Vec<(Vec<u8>, Option<Vec<u8>>)>);

/// A statement's answer and the keyspace entries it changes, as a golden body.
pub(super) fn render_defined(outcome: &Outcome) -> String {
	format!(
		"{}\nkeyspace:\n{}",
		super::golden::mask_system_metrics(&render_outcome(&outcome.0)),
		render_changes(&outcome.1)
	)
}

/// The keyspace of a quiescent datastore changes only by the statements run
/// on it, which is what lets two runs' keyspaces be compared: the index
/// compaction queue its setup's full-text write fills stays queued, where a
/// datastore running its maintenance tasks drains it on its own schedule.
#[tokio::test(flavor = "multi_thread")]
async fn a_quiescent_datastore_changes_only_by_the_statements_it_runs() {
	const SETUP: &str = "DEFINE ANALYZER words TOKENIZERS blank; \
		DEFINE INDEX only_text ON texts FIELDS t FULLTEXT ANALYZER words BM25; \
		CREATE texts:1 SET t = 'some words'";
	let queued = |keys: &[(Vec<u8>, Vec<u8>)]| {
		keys.iter().filter(|(key, _)| key.windows(3).any(|w| w == b"!ic")).count()
	};
	let quiet = TestDb::quiescent(SETUP, false).await;
	let before = keyspace(&quiet).await;
	assert!(queued(&before) > 0, "the full-text write queued no compaction");
	// Past the debounce a commit's compaction waits out, several times over.
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	assert_eq!(before, keyspace(&quiet).await, "the quiescent datastore changed on its own");
	let maintained = TestDb::new(SETUP).await;
	let mut drained = false;
	for _ in 0..200 {
		if queued(&keyspace(&maintained).await) == 0 {
			drained = true;
			break;
		}
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
	assert!(drained, "the maintenance tasks left the compaction queue as the setup filled it");
}

#[tokio::test]
async fn a_schema_statement_answers_and_changes_the_keyspace() {
	let mut golden = Golden::new("a_schema_statement_answers_and_changes_the_keyspace");
	for sql in SCHEMA_STATEMENTS {
		let parsed = parse(sql);
		golden.record(sql, render_defined(&defined((sql, &parsed)).await));
	}
	golden.check();
}

/// `DEFINE FIELD`'s checks of its definition are decided when the statement
/// is lowered: a definition they refuse lowers to the refusals it raises, at
/// the points the body raises them, and one they admit to none.
#[tokio::test]
async fn a_field_definition_is_checked_when_it_is_lowered() {
	use super::program::{FieldRefusal, Instr, StoreInstr};
	fn refusals(program: &super::program::Program, out: &mut Vec<FieldRefusal>) {
		for pipeline in program.pipelines.iter().chain(&program.host_code) {
			for instr in &pipeline.code {
				if let Instr::Store(StoreInstr::RefuseField {
					refusal,
				}) = instr
				{
					out.push(refusal.clone());
				}
			}
		}
		for unit in program.units.iter() {
			refusals(unit, out);
		}
	}
	let cases = [
		("DEFINE FIELD fresh ON person TYPE int", vec![]),
		(
			"DEFINE FIELD existing ON person COMPUTED 1",
			vec![FieldRefusal::ComputedFieldCannotBeIndexed {
				field: "existing".to_owned(),
				index: "by_existing".to_owned(),
			}],
		),
		(
			"DEFINE FIELD nested.deep ON person COMPUTED 1",
			vec![FieldRefusal::ComputedNestedField("nested.deep".to_owned())],
		),
		(
			"DEFINE FIELD existing.sub ON person TYPE int",
			vec![FieldRefusal::MismatchedFieldTypes {
				name: "existing.sub".to_owned(),
				kind: "int".to_owned(),
				existing_name: "existing".to_owned(),
				existing_kind: "string".to_owned(),
			}],
		),
	];
	for (sql, expected) in cases {
		let db = TestDb::new(DEFINED_SCHEMA).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let bound = bind(&ctx, sql).await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		let mut found = Vec::new();
		refusals(&program, &mut found);
		assert_eq!(found, expected, "`{sql}`");
		ctx.txn().cancel().await.expect("cancel");
	}
}

/// Views defined over rows that exist, each with the view it defines and how
/// many rows the backfill leaves in it.
const BACKFILLED_VIEWS: &[(&str, &str, usize)] = &[
	("DEFINE TABLE v AS SELECT * FROM sale", "v", 4),
	("DEFINE TABLE v AS SELECT region, amount FROM sale WHERE amount > 5", "v", 3),
	("DEFINE TABLE v AS SELECT region, amount * 2 AS doubled FROM sale", "v", 4),
	("DEFINE TABLE v AS SELECT region, count() AS n FROM sale GROUP BY region", "v", 2),
	("DEFINE TABLE v AS SELECT count() AS n, math::sum(amount) AS t FROM sale GROUP ALL", "v", 1),
	("DEFINE TABLE v AS SELECT region, math::mean(amount) AS m FROM sale GROUP BY region", "v", 2),
	(
		"DEFINE TABLE v AS SELECT region, math::variance(amount) AS a, math::stddev(amount) AS b FROM sale GROUP BY region",
		"v",
		2,
	),
	(
		"DEFINE TABLE v AS SELECT region, math::max(amount) AS hi, math::min(amount) AS lo, time::max(at) AS late FROM sale GROUP BY region",
		"v",
		2,
	),
	(
		"DEFINE TABLE v AS SELECT region, count() AS n FROM sale WHERE amount > 5 GROUP BY region",
		"v",
		2,
	),
	(
		"DEFINE TABLE v AS SELECT region, count() AS n FROM sale WHERE amount > 100 GROUP BY region",
		"v",
		0,
	),
	("DEFINE TABLE v AS SELECT count() AS n FROM sale WHERE amount > 100 GROUP ALL", "v", 0),
	("DEFINE TABLE OVERWRITE by_person AS SELECT existing FROM person", "by_person", 1),
];

/// A backfilled view holds a row for each record or group the rows that exist
/// give it.
#[tokio::test]
async fn a_view_is_filled_from_the_rows_that_exist() {
	async fn fill(sql: &str, view: &str) -> Vec<Value> {
		let db = TestDb::new(DEFINED_SCHEMA).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let bound = bind_parsed(&ctx, parse(sql)).await;
		let mut planner = planner(&ctx);
		planner.speculate_with(ctx.options().map(|options| Arc::new(options.clone())));
		let op = super::plan(&planner, &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		outcome(&op, &ctx).await.expect("the view is defined");
		let bound = bind(&ctx, &format!("SELECT * FROM {view} ORDER BY id")).await;
		let op = super::plan(&super::tests::planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined the read of `{view}`"));
		outcome(&op, &ctx).await.expect("the view reads")
	}
	let mut golden = Golden::new("a_view_is_filled_from_the_rows_that_exist");
	for (sql, view, rows) in BACKFILLED_VIEWS {
		let filled = fill(sql, view).await;
		assert_eq!(filled.len(), *rows, "`{sql}` filled {} rows", filled.len());
		golden.rows(sql, &Ok(filled));
	}
	golden.check();
}

/// Blocks, `RETURN`, `FOR` and `SLEEP`, each run as [`defined`] runs a
/// schema statement: the answer and the keyspace entries the statement changes.
pub(super) const CONTROL_STATEMENTS: &[&str] = &[
	"RETURN 1",
	"RETURN 1 + 2",
	"RETURN [1, 2, 3]",
	"RETURN (SELECT * FROM person)",
	"RETURN SELECT existing FROM person",
	"RETURN { RETURN 5; 6 }",
	"{}",
	"{ 1; 2; 3 }",
	"{ LET $x = 1; RETURN $x + 1 }",
	"{ LET $x: int = '5'; $x * 2 }",
	"{ LET $x: int = 'a'; $x }",
	"{ LET $t = 'person'; SELECT existing FROM type::table($t) }",
	"{ CREATE thing:2 SET existing = 'b'; SELECT existing FROM thing ORDER BY existing }",
	"{ DEFINE TABLE blocky; CREATE blocky:1; SELECT * FROM blocky }",
	"{ LET $x = (SELECT VALUE existing FROM person); RETURN $x }",
	"{ LET $x = 1; { LET $x = 2 }; $x }",
	"FOR $i IN [1, 2, 3] { CREATE type::record('counter', $i) SET n = $i }",
	"FOR $i IN 1..6 { IF $i = 2 { CONTINUE }; IF $i = 4 { BREAK }; CREATE type::record('counter', $i) }",
	"FOR $i IN 'x' { CREATE counter }",
	"FOR $i IN [] { CREATE counter }",
	"{ FOR $i IN [1, 2] { CREATE type::record('n', $i) }; RETURN (SELECT VALUE id FROM n ORDER BY id) }",
	"FOR $t IN ['a', 'b'] { DEFINE TABLE type::table($t); CREATE type::record($t, 1) }",
	"SLEEP 1ms",
	// A method the receiver's type has no builtin for calls the closure the
	// receiver holds under its name, or raises.
	"{ LET $o = { uppercase: |$x| $x * 2 }; RETURN $o.uppercase(3) }",
	"{ LET $o = { uppercase: 1 }; RETURN $o.uppercase(3) }",
	"{ LET $o = { k: 'v' }; RETURN $o.uppercase() }",
	"RETURN person:[(RETURN 1)]",
	"RETURN (SELECT VALUE name FROM person ORDER BY name).len()",
	"{ LET $x = (SELECT VALUE id FROM person ORDER BY id); RETURN $x[0] }",
	"{ LET $x = ({ RETURN 3 }) + 1; RETURN $x }",
	"RETURN (CREATE thing:9 SET v = 1).v",
	"FOR $p IN (SELECT VALUE id FROM person ORDER BY id) { CREATE type::record('counter', $p.id()) }",
	// Loops lowered into the program: jumps for `BREAK` and `CONTINUE` from
	// blocks within the body and signals from statements, nested loops, and
	// a body's `LET` scoped to its iteration.
	"FOR $i IN 1..6 { { IF $i = 3 { BREAK } }; CREATE type::record('counter', $i) }",
	"FOR $i IN 1..6 { { LET $j = $i * 2; { IF $j = 4 { CONTINUE } } }; CREATE type::record('counter', $i) }",
	"FOR $i IN 1..4 { FOR $j IN 1..4 { IF $j = 2 { CONTINUE }; IF $j = 3 { BREAK }; CREATE type::record('pair', [$i, $j]) }; CREATE type::record('outer', $i) }",
	"{ LET $x = 0; FOR $i IN 1..4 { LET $x = $i; CREATE type::record('seen', $i) SET x = $x }; RETURN $x }",
	"{ FOR $i IN 1..10 { IF $i = 3 { RETURN $i * 10 }; CREATE type::record('counter', $i) }; RETURN 'unreached' }",
	"FOR $i IN 1..4 { CREATE type::record('counter', $i) SET n = $i; UPDATE counter SET m = $i WHERE n = $i }",
	"{ LET $a = 1; { LET $b = $a + 1; { LET $c = $b + 1; CREATE type::record('deep', $c) } }; RETURN (SELECT VALUE id FROM deep) }",
	"{ CREATE counter:1 SET n = 1; DEFINE FIELD m ON counter VALUE 5; CREATE counter:2 SET n = 2; RETURN (SELECT * FROM counter ORDER BY id) }",
	"FOR $i IN 1..3 { BREAK; CREATE type::record('counter', $i) }",
	"FOR $i IN 1..3 { CONTINUE; CREATE type::record('counter', $i) }",
	"CREATE thing:[$limit, 'x'.len()] SET a = 1",
	// `RETURN … FETCH`: into an expression's value, and into each row of a
	// read, under the read's own `FETCH` and tail.
	"RETURN { who: person:1, gone: person:9 } FETCH who, gone",
	"{ LET $p = [{ who: person:1 }]; RETURN $p FETCH who }",
	"FOR $i IN [1] { RETURN { who: type::record('person', $i) } FETCH who }",
	"RETURN SELECT id, (SELECT VALUE id FROM person) AS all FROM person FETCH all",
	"RETURN SELECT * FROM ONLY sale:1 FETCH region",
	"RETURN (SELECT { p: id } AS w FROM person FETCH w) FETCH w.p",
	"RETURN CREATE thing:f SET p = person:1 FETCH p",
	"RETURN UPDATE person SET seen = true RETURN id, { p: id } AS w FETCH w.p",
];

#[tokio::test]
async fn blocks_answer_and_change_the_keyspace() {
	let mut golden = Golden::new("blocks_answer_and_change_the_keyspace");
	for sql in CONTROL_STATEMENTS {
		let parsed = parse(sql);
		golden.record(sql, render_defined(&defined((sql, &parsed)).await));
	}
	golden.check();
}

/// Blocks whose statements are compiled into the block's program as groups
/// of its own pipelines, run over the block's state: tails, state a
/// statement starts afresh on each reach (paging, sorts, aggregates, a
/// write's), signals and errors raised from within a statement, and a
/// statement compiled again once the block changes the catalog.
pub(super) const FLAT_CONTROL_STATEMENTS: &[&str] = &[
	"{ LET $p = SELECT * FROM ONLY person:1; RETURN $p }",
	"{ LET $p = SELECT * FROM ONLY person; RETURN $p }",
	"{ LET $p = SELECT * FROM ONLY person WHERE existing = 'zz'; RETURN $p }",
	"{ CREATE person:2 SET existing = 'b'; LET $p = SELECT * FROM ONLY person; RETURN $p }",
	"FOR $i IN 1..3 { SELECT * FROM ONLY person }",
	"{ LET $x = SELECT existing FROM person TIMEOUT 1s; RETURN $x }",
	"{ LET $x = CREATE ONLY counter:1 TIMEOUT 1s; RETURN $x }",
	"{ FOR $i IN 1..4 { CREATE type::record('counter', $i) SET n = $i TIMEOUT 5s }; RETURN SELECT * FROM counter ORDER BY id }",
	"{ LET $x = SELECT * FROM person WHERE sleep(30ms) = NONE TIMEOUT 1ms; RETURN $x }",
	"FOR $i IN 1..3 { SELECT * FROM person WHERE sleep(30ms) = NONE TIMEOUT 1ms }",
	"{ FOR $i IN 1..5 { CREATE type::record('counter', $i) SET n = $i; LET $c = SELECT count() AS c FROM counter GROUP ALL; LET $s = SELECT VALUE n FROM counter ORDER BY n DESC LIMIT 2; CREATE type::record('seen', $i) SET c = $c, s = $s }; RETURN SELECT * FROM seen ORDER BY id }",
	"{ FOR $i IN 1..4 { CREATE type::record('counter', $i) }; FOR $i IN 0..4 { LET $r = SELECT VALUE id FROM counter ORDER BY id LIMIT $i START 1; CREATE type::record('lim', $i) SET r = $r }; RETURN SELECT * FROM lim ORDER BY id }",
	"{ FOR $i IN 1..7 { UPSERT type::record('counter', $i % 2) SET n += 1 }; RETURN SELECT * FROM counter ORDER BY id }",
	"{ FOR $i IN 1..3 { LET $c = CREATE ONLY type::record('counter', $i); CREATE type::record('copy', $i) SET c = $c.id }; RETURN SELECT * FROM copy ORDER BY id }",
	"{ FOR $i IN 1..4 { LET $x = (IF $i = 2 { RETURN 'early' } ELSE { $i }); CREATE type::record('counter', $i) SET x = $x }; RETURN 'late' }",
	"FOR $i IN 1..4 { CREATE type::record('counter', $i); IF $i = 2 { THROW 'stopped' } }",
	"FOR $i IN 1..4 { CREATE counter:1 }",
	"{ FOR $i IN 1..5 { CREATE type::record('counter', $i) SET n = $i; DEFINE FIELD OVERWRITE m ON counter VALUE 7 }; RETURN SELECT * FROM counter ORDER BY id }",
	"{ FOR $i IN 1..4 { LET $l = SELECT * FROM ONLY late:1; CREATE type::record('late', $i) SET prev = $l.id TIMEOUT 5s; DEFINE FIELD OVERWRITE seen ON late VALUE true }; RETURN SELECT * FROM late ORDER BY id }",
	"{ LET $i = 10; FOR $i IN 1..3 { FOR $j IN 1..3 { LET $i = $i * 100 + $j; CREATE type::record('pair', $i) } }; RETURN [$i, (SELECT VALUE id FROM pair ORDER BY id)] }",
	"FOR $i IN 1..4 { IF $i = 3 { BREAK }; LET $x = SELECT VALUE existing FROM person; IF $x = ['a'] { CONTINUE }; CREATE counter }",
	// Names bound in slots: shadowing, reads from statements and from the
	// block's own expressions, and a statement compiled again after the
	// catalog changed, which reads them from the frame.
	"{ LET $acc = []; FOR $i IN 0..3 { LET $acc = $acc + [$i] }; RETURN $acc }",
	"{ LET $x = 5; { LET $x = $x + 1; LET $y = SELECT VALUE $x FROM person; RETURN [$x, $y] } }",
	"{ FOR $i IN 0..3 { LET $r = SELECT VALUE existing FROM person WHERE $i > 0; IF $r = ['a'] { RETURN [$i, $r] } }; RETURN 'none' }",
	"{ FOR $i IN 0..3 { LET $r = SELECT VALUE string::len(existing) + $i FROM person; IF $i = 2 { RETURN $r } } }",
	"{ DEFINE TABLE fresh; FOR $i IN 0..3 { LET $r = SELECT VALUE [existing, $i] FROM person WHERE $i > 0; IF $i = 2 { RETURN $r } } }",
	"{ FOR $i IN 0..3 { FOR $j IN 0..3 { LET $k = $i * 10 + $j; IF $k = 21 { RETURN SELECT VALUE [$k, $i, $j] FROM person } } } }",
];

#[tokio::test]
async fn flat_blocks_answer_and_change_the_keyspace() {
	let mut golden = Golden::new("flat_blocks_answer_and_change_the_keyspace");
	for sql in FLAT_CONTROL_STATEMENTS {
		let parsed = parse(sql);
		golden.record(sql, render_defined(&defined((sql, &parsed)).await));
	}
	golden.check();
}

/// Removals whose walks reach subscriptions, each run over live queries on
/// two tables, one owned by root and two by a database user.
pub(super) const KILLING_STATEMENTS: &[&str] = &[
	"REMOVE TABLE watched",
	"REMOVE TABLE AND EXPUNGE other",
	"REMOVE DATABASE test",
	"REMOVE NAMESPACE test",
	"REMOVE USER watcher ON DATABASE",
];

/// Run `sql` as [`defined`] does, with live queries open: its result, the
/// keyspace it changes, and the notifications its commit sends, each naming
/// its live query by the order the queries were opened.
async fn killed(
	sql: &str,
) -> (Result<Vec<Value>, String>, Vec<(Vec<u8>, Option<Vec<u8>>)>, Vec<(usize, String)>) {
	use crate::iam::{Actor, Auth, Level, Role};
	let (db, notifications) = TestDb::quiescent_with_notifications(
		"DEFINE TABLE watched; DEFINE TABLE other; \
		 DEFINE USER watcher ON DATABASE PASSWORD 'pass' ROLES VIEWER",
	)
	.await;
	let owner = TestDb::owner().with_rt(true);
	let mut watcher = TestDb::owner().with_rt(true);
	watcher.au = Arc::new(Auth::new(Actor::new(
		"watcher".to_owned(),
		vec![Role::Viewer],
		Level::Database("test".to_owned(), "test".to_owned()),
	)));
	let mut lives = Vec::new();
	for (session, live) in [
		(&owner, "LIVE SELECT * FROM watched"),
		(&watcher, "LIVE SELECT * FROM watched"),
		(&watcher, "LIVE SELECT * FROM other"),
	] {
		let mut responses =
			db.datastore().execute(live, session, None).await.expect("the live query runs");
		match responses.remove(0).result.expect("the live query opens") {
			crate::types::PublicValue::Uuid(id) => lives.push(id),
			other => panic!("a live query answered {other:?}"),
		}
	}
	let before = keyspace(&db).await;
	let ctx = db.exec_ctx_with_broker(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let rows = outcome(&op, &ctx).await;
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let after = keyspace(&db).await;
	let node = *db.datastore().id().as_bytes();
	// A live query's id is random, so it is named by when it was opened.
	let named = |bytes: &[u8]| {
		let mut out = without_node(bytes, &node);
		for (at, id) in lives.iter().enumerate() {
			let mut i = 0;
			while i + 16 <= out.len() {
				if out[i..i + 16] == *id.as_bytes() {
					out[i..i + 16].fill(at as u8 + 1);
					i += 16;
				} else {
					i += 1;
				}
			}
		}
		out
	};
	let mut changed: Vec<_> = changes(&before, &after)
		.into_iter()
		.map(|(key, value)| (named(&key), value.map(|v| named(&v))))
		.collect();
	changed.sort();
	let mut sent = Vec::new();
	while let Ok(notification) = notifications.try_recv() {
		let live = lives.iter().position(|id| *id == notification.id).unwrap_or(usize::MAX);
		sent.push((live, format!("{:?}", notification.action)));
	}
	sent.sort();
	(rows, changed, sent)
}

#[tokio::test]
async fn a_removal_kills_the_subscriptions_its_walk_reaches() {
	let mut golden = Golden::new("a_removal_kills_the_subscriptions_its_walk_reaches");
	for sql in KILLING_STATEMENTS {
		let (rows, changed, sent) = killed(sql).await;
		assert!(!sent.is_empty(), "`{sql}` killed nothing");
		let sent: Vec<String> =
			sent.iter().map(|(live, action)| format!("live query {live}: {action}")).collect();
		golden.record(
			sql,
			format!(
				"{}\nkeyspace:\n{}notifications:\n{}",
				render_outcome(&rows),
				render_changes(&changed),
				sent.join("\n")
			),
		);
	}
	golden.check();
}

pub(super) async fn bind(ctx: &ExecutionContext, sql: &str) -> LogicalOp {
	bind_parsed(ctx, parse(sql)).await
}

/// The one statement `sql` holds, parsed and lowered.
pub(super) fn parse(sql: &str) -> LogicalOp {
	let ast = crate::syn::parse(sql).expect("statement parses");
	let crate::sql::TopLevelExpr::Expr(expr) =
		ast.expressions.into_iter().next().expect("one statement")
	else {
		panic!("not an expression statement: {sql}");
	};
	crate::expr::logical::lower(expr).expect("statement lowers")
}

pub(super) async fn bind_parsed(ctx: &ExecutionContext, op: LogicalOp) -> LogicalOp {
	planner(ctx).bind(op).await.expect("statement binds")
}

/// The blocks of the write and schema corpora a planning that models a block's
/// schema statements ahead declines whole
/// (`vm::speculate::DATA_DEPENDENT_DDL`), each with whether a record user's
/// planning declines it too: each runs a statement that reads a catalog change
/// the block cannot model ahead. The VM plans such a block again, with what
/// follows the change late-bound, so it runs there all the same; a lowering
/// that models ahead ([`super::lower::lower`]) still declines it. A record user may not change the
/// catalog, so a block whose schema statement is known to raise for one folds what follows it and
/// is lowered.
pub(super) const DECLINED_WHOLE: &[(&str, bool)] = &[
	// A schema statement every iteration of a loop runs, or one an iteration
	// decides to run, before what the loop's next iteration reads.
	(
		"{ FOR $i IN 0..3 { DEFINE FIELD OVERWRITE m ON counter VALUE $i; CREATE type::record('counter', $i) }; RETURN SELECT * FROM counter ORDER BY id }",
		true,
	),
	(
		"{ FOR $i IN 0..3 { IF $i = 1 { DEFINE FIELD m ON counter VALUE 'one' }; CREATE type::record('counter', $i) }; RETURN SELECT * FROM counter ORDER BY id }",
		true,
	),
	(
		"{ FOR $i IN 0..3 { IF $i = 1 { DEFINE TABLE later; CREATE later:1 } ELSE { CREATE type::record('counter', $i) } }; RETURN [(SELECT * FROM later), (SELECT * FROM counter ORDER BY id)] }",
		true,
	),
	(
		"{ FOR $i IN 1..4 { LET $l = SELECT * FROM ONLY late:1; CREATE type::record('late', $i) SET prev = $l.id TIMEOUT 5s; DEFINE FIELD OVERWRITE seen ON late VALUE true }; RETURN SELECT * FROM late ORDER BY id }",
		true,
	),
	(
		"{ FOR $i IN 1..5 { CREATE type::record('counter', $i) SET n = $i; DEFINE FIELD OVERWRITE m ON counter VALUE 7 }; RETURN SELECT * FROM counter ORDER BY id }",
		true,
	),
	(
		"FOR $i IN 0..4 { CREATE type::record('item', 80 + $i) SET name = 'l', extra = $i; IF $i = 1 { DEFINE INDEX loop_ix ON item FIELDS extra } }",
		true,
	),
	// A schema statement a condition on data decides to run, before a field
	// definition its index refuses.
	(
		"{ IF (SELECT VALUE existing FROM ONLY person:1) = 'a' { DEFINE INDEX by_calc ON person FIELDS calc }; DEFINE FIELD calc ON person COMPUTED 1 }",
		true,
	),
	// A schema statement whose name a loop variable gives.
	("FOR $t IN ['a', 'b'] { DEFINE TABLE type::table($t); CREATE type::record($t, 1) }", true),
];

/// The blocks a planning that models a block's schema statements ahead
/// declines whole for a record user only: the owner's schema statements are
/// modelled, and a record user's raise where a condition on data decides which
/// of the block's later statements run.
pub(super) const DECLINED_WHOLE_FOR_RECORDS: &[&str] = &[
	"FOR $i IN 0..3 { DEFINE FIELD IF NOT EXISTS owner ON fresh2 TYPE option<record<root>> REFERENCE ON DELETE CASCADE; CREATE type::record('fresh2', $i) SET owner = root:3; IF $i = 1 { DELETE root:3 } }",
];

/// Whether [`DECLINED_WHOLE`] and [`DECLINED_WHOLE_FOR_RECORDS`] list block
/// `sql` as declined whole for a run by a record user (`record_user`) or by
/// any other principal.
pub(super) fn declined_whole(sql: &str, record_user: bool) -> bool {
	DECLINED_WHOLE
		.iter()
		.any(|(listed, for_records)| *listed == sql && (*for_records || !record_user))
		|| record_user && DECLINED_WHOLE_FOR_RECORDS.contains(&sql)
}

/// `bound`, statement `sql`, planned onto the VM by `planner`, as the executor
/// plans it: a block's schema statements it cannot model ahead are planned when
/// the block reaches them.
pub(super) async fn planned(
	planner: &Planner<'_>,
	bound: &LogicalOp,
	sql: &str,
) -> Arc<dyn ExecOperator> {
	super::plan(planner, bound).await.unwrap_or_else(|| panic!("the VM declined `{sql}`"))
}

/// `program` with the bundles of the stages each of its writes that may reach
/// any table would load by name appended to its arena: one per table of the
/// database, and the generic bundle of a table the catalog does not hold, as
/// `planner` lowers them, so a census of its instructions sees the stages a
/// run of it loads.
pub(super) async fn with_stages(
	planner: &Planner<'_>,
	program: &super::program::Program,
) -> super::program::Program {
	use crate::catalog::providers::TableProvider;
	fn named(program: &super::program::Program, out: &mut Vec<Arc<super::units::StageSetUnit>>) {
		for spec in
			program.write.iter().chain(program.groups.iter().filter_map(|g| g.write.as_ref()))
		{
			if let Some(set) = spec.tables.as_ref().and_then(|tables| tables.by_name.clone()) {
				out.push(set);
			}
		}
		for unit in program.units.iter() {
			named(unit, out);
		}
	}
	let mut sets = Vec::new();
	named(program, &mut sets);
	let mut program = program.clone();
	if sets.is_empty() {
		return program;
	}
	let txn = planner.txn().cloned().expect("a transaction");
	let (ns, db) = planner.ns_db_ids().await.expect("a database");
	// A transaction already finished lists nothing: the census takes the
	// program as it is.
	let Ok(tables) = txn.all_tb(ns, db, None).await else {
		return program;
	};
	let tables: Vec<crate::val::TableName> = tables.iter().map(|t| t.name.clone()).collect();
	let mut units = program.units.to_vec();
	for set in &sets {
		for table in &tables {
			if let Ok((bundle, _)) =
				super::lower::stage_bundle_for(planner, set, table, false).await
			{
				units.push(bundle);
			}
		}
		let absent = crate::val::TableName::from("absent_table_for_census".to_owned());
		if let Ok((bundle, _)) = super::lower::stage_bundle_for(planner, set, &absent, true).await {
			units.push(bundle);
		}
	}
	program.units = units.into();
	program
}

/// The families of write instructions `program` runs, in its own pipelines
/// and host pipelines and in every unit of its arena: `data`, `gate`,
/// `schema`, `observe` and `storage` for the stages that lower to them, and
/// `output` for the images a write shows.
pub(super) fn write_families(program: &super::program::Program) -> Vec<&'static str> {
	use super::program::{Instr, WriteInstr};
	fn walk(program: &super::program::Program, out: &mut Vec<&'static str>) {
		for pipeline in program.pipelines.iter().chain(&program.host_code) {
			for instr in &pipeline.code {
				let Instr::Write(write) = instr else {
					continue;
				};
				let family = match write {
					WriteInstr::Data(_) => "data",
					WriteInstr::Permissions(_) => "gate",
					WriteInstr::Schema(_) => "schema",
					WriteInstr::Observe(_) => "observe",
					WriteInstr::Storage(_) => "storage",
					WriteInstr::ShowImage {
						..
					}
					| WriteInstr::SetOutput {
						..
					} => "output",
					_ => continue,
				};
				if !out.contains(&family) {
					out.push(family);
				}
			}
		}
		for unit in program.units.iter() {
			walk(unit, out);
		}
	}
	let mut out = Vec::new();
	walk(program, &mut out);
	out.sort_unstable();
	out
}

/// The family of write instructions stage `id` lowers to, as
/// [`write_families`] names them.
pub(super) fn family_of(id: crate::exec::operators::write::stage::StageId) -> &'static str {
	use crate::exec::operators::write::stage::StageId;
	match id {
		StageId::OutputWrite => "output",
		StageId::CheckWhereCondition
		| StageId::ComputeInputData
		| StageId::CheckDataFields
		| StageId::ProcessRecordData
		| StageId::ProcessMergeData
		| StageId::DefaultRecordData
		| StageId::GenerateRecordId => "data",
		StageId::CheckRecordUserAccess
		| StageId::CheckPermissionsQuickCreate
		| StageId::CheckUpdatePermissions
		| StageId::CheckDeletePermissions
		| StageId::CheckCreatePermissions
		| StageId::RecheckUpdatePermissions
		| StageId::CheckSelectPermissions(_) => "gate",
		StageId::ProcessTableFields
		| StageId::CleanupTableFields
		| StageId::MaterialiseObservedFields => "schema",
		StageId::ProcessTableEvents(_)
		| StageId::ProcessChangefeeds
		| StageId::ProcessTableLives(_)
		| StageId::ProcessTableViews(_) => "observe",
		StageId::CheckRecordExists
		| StageId::CheckTableNotView
		| StageId::CheckTableNotLightweight(_)
		| StageId::CheckTableType(_)
		| StageId::ClearRecordData
		| StageId::StoreRecordData
		| StageId::StoreEdgesData
		| StageId::StoreInlineAdjacencyData
		| StageId::ProcessTableReferences
		| StageId::CleanupTableReferences
		| StageId::StoreIndexData
		| StageId::ReclaimDocId
		| StageId::PurgeRecordData => "storage",
	}
}

/// A planner for a statement of `ctx`, as the executor plans one it is about
/// to run: speculating, so a block's schema statements are modelled.
pub(super) fn planner(ctx: &ExecutionContext) -> Planner<'_> {
	let mut planner = Planner::with_txn(
		ctx.ctx(),
		ctx.function_registry(),
		ctx.try_txn().expect("a transaction"),
		Some("test".to_owned()),
		Some("test".to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth));
	planner.speculate_with(ctx.options().map(|options| Arc::new(options.clone())));
	planner
}

pub(super) async fn outcome(
	op: &Arc<dyn ExecOperator>,
	ctx: &ExecutionContext,
) -> Result<Vec<Value>, String> {
	use crate::expr::ControlFlow;
	// A statement ends as the executor ends it: a `RETURN` with its value, and
	// a `BREAK` or `CONTINUE` that no loop caught with the error it raises.
	match try_collect(op, ctx).await {
		Ok(rows) => Ok(rows),
		Err(ControlFlow::Return(value)) => Ok(vec![value]),
		Err(ControlFlow::Err(e)) => Err(format!("{e:#}")),
		Err(ControlFlow::Break | ControlFlow::Continue) => {
			Err(crate::exec::Error::InvalidControlFlow.to_string())
		}
	}
}

#[tokio::test]
async fn the_vm_answers_every_statement_it_covers() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let mut golden = Golden::new("the_vm_answers_every_statement_it_covers");
	let mut declined = Vec::new();
	for sql in COVERED {
		let bound = bind(&ctx, sql).await;
		let Some(vm) = super::plan(&planner(&ctx), &bound).await else {
			declined.push(*sql);
			continue;
		};
		let mut answer = outcome(&vm, &ctx).await;
		if sql.contains("RAND()") {
			// The rows come in a random order and are recorded sorted.
			if let Ok(rows) = &mut answer {
				rows.sort();
			}
		}
		golden.rows(sql, &answer);
	}
	assert!(declined.is_empty(), "the VM declined {declined:#?}");
	golden.check();
}

#[tokio::test]
async fn the_vm_declines_what_it_does_not_cover() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	for sql in DECLINED {
		let bound = bind(&ctx, sql).await;
		assert!(super::plan(&planner(&ctx), &bound).await.is_none(), "the VM covered `{sql}`");
	}
}

/// A statement that is refused as it is planned — a `LET` of a protected name
/// up to the block's first schema statement, a `LET` written as an
/// expression, a grouped projection reading `$this`, a `FOR UPDATE` over
/// targets that are not records or under clauses that drop its rows — is
/// refused by the VM's planning with an error, rather than declined.
#[tokio::test]
async fn the_vm_refuses_what_cannot_be_run_as_it_plans() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let mut golden = Golden::new("the_vm_refuses_what_cannot_be_run_as_it_plans");
	for sql in [
		"{ CREATE refused:1; LET $auth = 1 }",
		"FOR $i IN [1] { LET $token = $i }",
		"IF true { LET $access = 1 } ELSE { NONE }",
		"{ LET $x = 1; { LET $session = $x } }",
		"{ LET $auth = 1; DEFINE TABLE refused_first }",
		"RETURN [LET $x = 1]",
		"SELECT { a: LET $x = 1 } AS a FROM person",
		"{ CREATE refused:2; RETURN type::is_int(LET $x = 1) }",
		"SELECT $this AS row, count() FROM person GROUP ALL",
		"SELECT *, count() AS c FROM person GROUP BY age",
		"SELECT * FROM person FOR UPDATE",
		"SELECT * FROM [1] FOR UPDATE",
		"SELECT count() FROM person:1 GROUP ALL FOR UPDATE",
		"SELECT * FROM person:1, person:2 LIMIT 1 FOR UPDATE",
		"{ CREATE refused:3; SELECT * FROM person:1..3 FOR UPDATE }",
	] {
		let bound = bind(&ctx, sql).await;
		let refusal = match super::plan_checked(&planner(&ctx), &bound, None).await {
			Err(e) => e.to_string(),
			Ok(planned) => panic!("the VM planned `{sql}`: {:?}", planned.name()),
		};
		golden.record(sql, refusal);
	}
	golden.check();
}

/// A `FOR UPDATE` read of a record id, written or resolved when the statement
/// runs, is answered by the VM, and a value that is not a record id is refused
/// when the source opens.
#[tokio::test]
async fn a_locked_read_of_a_record_is_answered_and_a_non_record_refused() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let mut golden = Golden::new("a_locked_read_of_a_record_is_answered_and_a_non_record_refused");
	for sql in [
		"SELECT * FROM person:1 FOR UPDATE",
		"SELECT name FROM person:99 FOR UPDATE",
		"SELECT VALUE name FROM person:1 WHERE age > 100 FOR UPDATE",
		"SELECT * FROM type::record('person', 2) FOR UPDATE",
		"{ LET $r = person:3; SELECT name FROM $r FOR UPDATE }",
		"{ LET $r = person:3; SELECT name FROM $r LIMIT 0 FOR UPDATE }",
		"{ LET $r = [1]; SELECT * FROM $r FOR UPDATE }",
		"{ LET $r = person:1..3; SELECT * FROM $r FOR UPDATE }",
		"{ LET $r = person; SELECT * FROM $r FOR UPDATE }",
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		golden.record(
			sql,
			format!("{:?}\n{}", vm.access_mode(), render_outcome(&outcome(&vm, &ctx).await)),
		);
	}
	golden.check();
}

/// A principal pinned to another tenant is refused before anything is read,
/// as every scan refuses it.
#[tokio::test]
async fn a_principal_outside_the_database_is_refused() {
	let db = TestDb::new(SCHEMA).await;
	let owner = db.exec_ctx().await;
	let bound = bind(&owner, "SELECT * FROM person").await;
	let vm = super::plan(&planner(&owner), &bound).await.expect("covered");
	let rid = crate::types::PublicValue::String("user:tobie".to_owned());
	let foreign = crate::dbs::Session {
		au: Arc::new(crate::iam::Auth::for_record(
			"user:tobie".to_owned(),
			"other",
			"test",
			"user",
		)),
		..crate::dbs::Session::for_record("test", "test", "user", rid)
	};
	let ctx = db.exec_ctx_as(&foreign, crate::kvs::TransactionType::Read).await;
	let refused = outcome(&vm, &ctx).await;
	let mut golden = Golden::new("a_principal_outside_the_database_is_refused");
	golden.rows("SELECT * FROM person", &refused);
	assert!(refused.is_err(), "{refused:?}");
	golden.check();
}

/// A record user reads through the table and field permissions, however the
/// executor runs the statement, and never sees a field a guard hides.
#[tokio::test]
async fn a_record_user_reads_through_the_permissions() {
	let mut golden = Golden::new("a_record_user_reads_through_the_permissions");
	let db = TestDb::new_with_auth(
		"
		DEFINE TABLE bag PERMISSIONS FULL;
		DEFINE FIELD arr ON bag TYPE array;
		DEFINE FIELD arr[*].secret ON bag PERMISSIONS FOR select NONE;
		DEFINE FIELD hidden ON bag PERMISSIONS FOR select NONE;
		CREATE bag:1 SET arr = [{ secret: 's1', shown: 1 }], hidden = 'h', open = 1;
		DEFINE TABLE plain PERMISSIONS FULL;
		CREATE plain:1 SET a = 1;
	",
	)
	.await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for sql in [
		"SELECT * FROM bag",
		"SELECT arr FROM bag",
		"SELECT VALUE hidden FROM bag",
		"SELECT * FROM bag WHERE open = 1",
		"SELECT * FROM plain",
	] {
		let mut responses = db.datastore().execute(sql, &reader, None).await.expect("runs");
		let expected: Result<Vec<Value>, String> = responses
			.remove(0)
			.result
			.map(|rows| vec![Value::from(rows)])
			.map_err(|error| error.to_string());
		golden.rows(sql, &expected);
		let shown = format!("{expected:?}");
		assert!(!shown.contains("s1") && !shown.contains("\"h\""), "`{sql}` shows {shown}");
	}
	golden.check();
}

/// A scan pipeline whose body evaluates nothing the VM has no instruction for
/// runs on the stored bytes; any other runs on decoded rows.
#[tokio::test]
async fn which_statements_read_the_stored_bytes() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	for (sql, in_place) in [
		("SELECT * FROM person WHERE age > 18 AND name = 'cat'", true),
		("SELECT name, age AS a FROM person WHERE age >= 1 LIMIT 3 START 1", true),
		("SELECT VALUE i FROM big WHERE n > 2", true),
		("SELECT * FROM person WHERE tags CONTAINS 'b'", true),
		("SELECT * FROM person WHERE id = person:1", true),
		("SELECT * FROM person, big WHERE n = 3", true),
		("SELECT * FROM person:1, big:1..5", true),
		("SELECT * FROM person WHERE string::len(name) > 2", false),
		("SELECT age + 1 AS b FROM person", true),
		("SELECT age > $limit AS b FROM person", false),
		("SELECT * FROM person ORDER BY age", true),
		("SELECT * FROM person WHERE string::len(name) > 2 ORDER BY age", false),
	] {
		let bound = bind(&ctx, sql).await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		assert_eq!(program.pipelines[0].in_place, in_place, "`{sql}`");
	}
}

/// The planner's access-path decision reaches the VM's plan: an indexed
/// predicate reads the index, a pinned record id is looked up, and an ordering
/// the index delivers needs no sort.
#[tokio::test]
async fn an_indexed_statement_reads_the_index_the_planner_chooses() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	fn source(plan: &Plan) -> &Plan {
		match plan.input() {
			Some(input) => source(input),
			None => plan,
		}
	}
	fn sorts(plan: &Plan) -> bool {
		matches!(plan, Plan::Sort { .. }) || plan.input().is_some_and(sorts)
	}
	for (sql, index, sorted) in [
		("SELECT * FROM indexed WHERE age = 3", Some("by_age"), false),
		("SELECT * FROM indexed WHERE age > 6 ORDER BY age DESC LIMIT 2", Some("by_age"), false),
		("SELECT * FROM indexed WHERE name = 'n7'", Some("by_name"), false),
		("SELECT * FROM indexed WHERE kind = 1 ORDER BY age", Some("by_pair"), false),
		("SELECT * FROM indexed WHERE age = 3 ORDER BY name", Some("by_age"), true),
		("SELECT kind AS age FROM indexed ORDER BY age", Some("by_age"), true),
		("SELECT id FROM indexed WHERE age IN [3, 4]", Some("union of by_age"), false),
		(
			"SELECT id FROM indexed WHERE age = 3 OR age = 5 ORDER BY id DESC",
			Some("union of by_age"),
			false,
		),
		("SELECT id FROM pairs WHERE kind IN [1, 2] ORDER BY age", Some("union of by_pair"), false),
		(
			"SELECT id FROM pairs WHERE kind IN [1, 2] ORDER BY age DESC",
			Some("union of by_pair"),
			false,
		),
		(
			"SELECT id FROM pairs WHERE kind IN [1, 2] ORDER BY id DESC",
			Some("union of by_pair"),
			true,
		),
		("SELECT * FROM indexed WITH NOINDEX WHERE age = 3", None, false),
		("SELECT * FROM indexed WHERE id = indexed:5", None, false),
		("SELECT * FROM indexed WHERE kind = 2 AND age = 5", Some("bitmap and"), false),
		("SELECT count() FROM big GROUP ALL", Some("count keys"), false),
		("SELECT count() FROM counted WHERE n < 5 GROUP ALL", Some("count index"), false),
		("SELECT count() FROM counted WHERE n = 3 GROUP ALL", Some("count btree"), false),
		(
			"SELECT count() FROM counted WHERE n = 3 AND m = 1 GROUP ALL",
			Some("count bitmap"),
			false,
		),
		(
			"SELECT id FROM indexed WHERE age = 3 OR name = 'n4' OR age = 5",
			Some("bitmap or"),
			false,
		),
	] {
		let bound = bind(&ctx, sql).await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let read = match source(&physical.root) {
			Plan::IndexScan {
				index,
				..
			} => Some(index.name.as_str().to_owned()),
			Plan::IndexUnion {
				branches,
				..
			} => Some(format!("union of {}", branches[0].index.name.as_str())),
			Plan::Count {
				count,
				..
			} => Some(match count {
				crate::exec::planner::CountDecision::All {
					..
				} => "count keys".to_owned(),
				crate::exec::planner::CountDecision::Indexed {
					btree_access: Some(_),
					..
				} => "count btree".to_owned(),
				crate::exec::planner::CountDecision::Indexed {
					bitmap_plan: Some(_),
					..
				} => "count bitmap".to_owned(),
				crate::exec::planner::CountDecision::Indexed {
					..
				} => "count index".to_owned(),
			}),
			Plan::Bitmap {
				fallback,
				..
			} => Some(if fallback.is_some() {
				"bitmap or".to_owned()
			} else {
				"bitmap and".to_owned()
			}),
			_ => None,
		};
		assert_eq!(read.as_deref(), index, "`{sql}` reads {:?}", source(&physical.root));
		assert_eq!(sorts(&physical.root), sorted, "`{sql}`");
	}
}

/// A comparison of a field with an integer literal is read typed, either way
/// round, unless the schema declares the field of a kind that holds no
/// integer.
#[tokio::test]
async fn which_comparisons_read_the_field_typed() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	for (sql, typed) in [
		("SELECT * FROM mixed WHERE n = 3", true),
		("SELECT * FROM mixed WHERE 3 < n", true),
		("SELECT * FROM big WHERE n > 2", true),
		("SELECT * FROM typed WHERE s = 3", false),
		("SELECT * FROM mixed WHERE n = 3f", false),
		("SELECT * FROM mixed WHERE n CONTAINS 3", false),
	] {
		let bound = bind(&ctx, sql).await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		let row = &program.pipelines[0];
		let found = row.code.iter().any(|i| {
			matches!(
				i,
				super::program::Instr::CompareFieldInt { .. }
					| super::program::Instr::JumpIfFieldInt { .. }
					| super::program::Instr::FilterFieldInt { .. }
			)
		});
		assert_eq!(found, typed, "`{sql}`");
	}
}

/// A record user reads a table through its `PERMISSIONS FOR select`: the VM
/// applies each kind of table permission, and runs the statement rather than
/// declining it.
#[tokio::test]
async fn a_table_permission_admits_the_rows_it_names() {
	let mut golden = Golden::new("a_table_permission_admits_the_rows_it_names");
	let db = TestDb::new_with_auth(
		"
		DEFINE TABLE user PERMISSIONS NONE;
		CREATE user:reader SET name = 'reader';
		CREATE user:other SET name = 'other';
		DEFINE TABLE owned PERMISSIONS FOR select WHERE owner = $auth.id;
		CREATE owned:1 SET owner = user:reader, v = 1;
		CREATE owned:2 SET owner = user:other, v = 2;
		CREATE owned:3 SET owner = user:reader, v = 3;
		DEFINE TABLE denied PERMISSIONS NONE;
		CREATE denied:1 SET v = 1;
		DEFINE TABLE for_reader PERMISSIONS FOR select WHERE $auth.id = user:reader;
		CREATE for_reader:1 SET v = 1;
		DEFINE TABLE for_other PERMISSIONS FOR select WHERE $auth.id = user:other;
		CREATE for_other:1 SET v = 1;
		DEFINE TABLE named PERMISSIONS FOR select WHERE string::starts_with(tag, 'a');
		CREATE named:1 SET tag = 'apple';
		CREATE named:2 SET tag = 'berry';
		DEFINE TABLE linked PERMISSIONS FOR select WHERE owner.name = 'reader';
		CREATE linked:1 SET owner = user:reader;
		CREATE linked:2 SET owner = user:other;
		DEFINE TABLE guarded PERMISSIONS FULL;
		DEFINE FIELD secret ON guarded PERMISSIONS FOR select NONE;
		DEFINE FIELD twice ON guarded COMPUTED v * 2;
		CREATE guarded:1 SET v = 1, secret = 's';
		DEFINE TABLE refs PERMISSIONS FULL;
		CREATE refs:1 SET a = owned:1, b = owned:2, c = denied:1, d = [owned:1, owned:2, denied:1],
			e = linked:2, f = { x: owned:3, y: [guarded:1] }, g = guarded:1, h = missing:1,
			i = for_other:1;
	",
	)
	.await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	let ctx = db.exec_ctx_as(&reader, crate::kvs::TransactionType::Read).await;
	for sql in [
		// A fetched record is read under its own table's permissions.
		"SELECT * FROM refs FETCH a, b, c",
		"SELECT * FROM refs FETCH d",
		"SELECT * FROM refs FETCH e, i",
		"SELECT * FROM refs FETCH f.x, f.y",
		"SELECT * FROM refs FETCH g",
		"SELECT * FROM refs FETCH h",
		"SELECT VALUE d FROM refs FETCH d",
		"SELECT * FROM owned FETCH owner",
		"SELECT * FROM owned",
		"SELECT v FROM owned WHERE v > 1",
		"SELECT count() FROM owned GROUP ALL",
		"SELECT * FROM owned ORDER BY v DESC LIMIT 1",
		"SELECT * FROM owned:1..4",
		"SELECT * FROM owned:2",
		"SELECT * FROM denied",
		"SELECT count() FROM denied GROUP ALL",
		"SELECT * FROM for_reader",
		"SELECT * FROM for_other",
		"SELECT * FROM named",
		"SELECT * FROM linked",
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		golden.rows(sql, &outcome(&vm, &ctx).await);
	}
	golden.check();
}

/// The cases above would pass if the VM emitted nothing and the golden recorded nothing.
#[tokio::test]
async fn a_covered_statement_returns_rows() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let bound = bind(&ctx, "SELECT VALUE i FROM big WHERE n = 0").await;
	let vm = super::plan(&planner(&ctx), &bound).await.expect("covered");
	let rows = outcome(&vm, &ctx).await.expect("runs");
	assert_eq!(rows.len(), 3000 / 7);
	assert_eq!(rows.first(), Some(&Value::from(7)));
}

#[tokio::test]
async fn a_plan_records_the_table_it_reads_at_its_catalog_version() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let bound = bind(&ctx, "SELECT * FROM person").await;
	let plan = super::map::map(&planner(&ctx), &bound).await.expect("maps");
	let [
		Dependency::Table {
			table,
			..
		},
	] = plan.dependencies.as_slice()
	else {
		panic!("one table dependency: {:?}", plan.dependencies);
	};
	assert_eq!(table.as_str(), "person");
}

#[tokio::test]
async fn a_sort_under_a_literal_limit_keeps_only_the_rows_the_limit_reaches() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let keep = |plan: &Plan| {
		let mut node = Some(plan);
		while let Some(n) = node {
			if let Plan::Sort {
				keep,
				..
			} = n
			{
				return *keep;
			}
			node = n.input();
		}
		panic!("no sort in {plan:?}");
	};
	for (sql, expected) in [
		("SELECT * FROM person ORDER BY age LIMIT 2 START 3", Some(5)),
		("SELECT * FROM person ORDER BY age LIMIT 2", Some(2)),
		("SELECT * FROM person ORDER BY age LIMIT $lim", None),
		("SELECT * FROM person ORDER BY age START 2", None),
	] {
		let bound = bind(&ctx, sql).await;
		let plan =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		assert_eq!(keep(&plan.root), expected, "`{sql}`");
	}
}

/// Time a filtered scan on the VM. Run in release:
/// `cargo test --release -p surrealdb-core --lib exec::vm::tests::throughput -- --ignored
/// --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn throughput() {
	const ROWS: usize = 200_000;
	let db = TestDb::new(&format!(
		"FOR $i IN 1..={ROWS} {{ CREATE type::record('t', $i) SET n = $i % 7, i = $i, s = 'row' }}"
	))
	.await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT * FROM t",
		"SELECT VALUE i FROM t WHERE n > 2",
		"SELECT i, s FROM t WHERE n = 3 AND i > 100",
		"SELECT * FROM t WHERE n = 3 ORDER BY i DESC LIMIT 10",
		"SELECT * FROM t ORDER BY i DESC LIMIT 10",
		"SELECT * FROM t ORDER BY i DESC",
		"SELECT i, s FROM t ORDER BY s, i DESC",
		"SELECT * FROM t ORDER BY i DESC LIMIT 100",
		"SELECT * FROM t ORDER BY i DESC LIMIT 1000",
		"SELECT * FROM t ORDER BY i DESC LIMIT 5000",
		"SELECT * FROM t ORDER BY i DESC LIMIT 20000",
		"SELECT * FROM t ORDER BY n DESC, i LIMIT 5000",
		"SELECT count() FROM t WHERE n = 3 GROUP ALL",
		"SELECT math::sum(i) AS s, math::max(i) AS hi FROM t WHERE n = 3 GROUP ALL",
		"SELECT math::sum(i) AS s FROM t GROUP ALL",
		"SELECT n, count() AS c, math::sum(i) AS s FROM t GROUP BY n",
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("covered");
		for _ in 0..5 {
			let started = web_time::Instant::now();
			let rows = outcome(&vm, &ctx).await.expect("runs").len();
			let elapsed = started.elapsed();
			println!(
				"{:>8.1} ns/row {rows:>7} rows  {sql}",
				elapsed.as_nanos() as f64 / ROWS as f64
			);
		}
	}
}

/// Split a scan's cost per row into the cursor, the record decode, and the
/// rest. Run in release, as `throughput`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn where_the_time_goes() {
	use std::borrow::Cow;

	use crate::exec::operators::scan::pipeline::decode_record;
	use crate::key::schema::RecordPrefix;

	const ROWS: usize = 200_000;
	let db = TestDb::new(&format!(
		"FOR $i IN 1..={ROWS} {{ CREATE type::record('t', $i) SET n = $i % 7, i = $i, s = 'row' }}"
	))
	.await;
	let ctx = db.exec_ctx().await;
	let db_ctx = ctx.database().expect("database");
	let range = || {
		RecordPrefix {
			ns: db_ctx.ns_ctx.ns.namespace_id,
			db: db_ctx.db.database_id,
			tb: Cow::Borrowed(&"t".into()),
		}
		.range()
		.expect("range")
	};
	let per_row = |started: web_time::Instant| started.elapsed().as_nanos() as f64 / ROWS as f64;
	for _ in 0..2 {
		let txn = ctx.txn();
		let started = web_time::Instant::now();
		let mut cursor = txn
			.open_vals_cursor_raw(range(), crate::kvs::Direction::Forward, 0, None)
			.await
			.expect("cursor");
		let mut bytes = 0usize;
		loop {
			let stats = cursor
				.for_each(1000, &mut |key, val| {
					bytes += key.len() + val.len();
					Ok(std::ops::ControlFlow::Continue(()))
				})
				.await
				.expect("scan");
			if stats.rows == 0 {
				break;
			}
		}
		println!("cursor only      {:>7.1} ns/row ({bytes} bytes)", per_row(started));

		let started = web_time::Instant::now();
		let mut cursor = txn
			.open_vals_cursor_raw(range(), crate::kvs::Direction::Forward, 0, None)
			.await
			.expect("cursor");
		let mut rows = Vec::with_capacity(ROWS);
		loop {
			let stats = cursor
				.for_each(1000, &mut |key, val| {
					rows.push(decode_record(key, val).expect("decodes"));
					Ok(std::ops::ControlFlow::Continue(()))
				})
				.await
				.expect("scan");
			if stats.rows == 0 {
				break;
			}
		}
		println!("cursor + decode  {:>7.1} ns/row", per_row(started));
		drop(rows);

		// The stored-bytes steps a row program takes for `n = 3`, one at a time.
		let n = super::row::FieldName::new("n");
		let three = crate::val::Number::Int(3);
		for step in ["locate n", "locate + decode n", "locate + decode + registers"] {
			let started = web_time::Instant::now();
			let mut cursor = txn
				.open_vals_cursor_raw(range(), crate::kvs::Direction::Forward, 0, None)
				.await
				.expect("cursor");
			let mut hits = 0usize;
			loop {
				let stats = cursor
					.for_each(1000, &mut |_key, val| {
						let data = crate::exec::object_extract::rev2_record_data_bytes(val)
							.expect("rev 2");
						let bytes = super::row::RevisionRow::field_bytes(data, &n)
							.expect("walkable")
							.expect("present");
						if step == "locate n" {
							hits += bytes.len() & 1;
						} else {
							let scalar = super::row::scalar_from_wire(bytes).expect("decodes");
							if step == "locate + decode + registers" {
								let regs: [super::scalar::Scalar<'_>;
									super::program::STACK_REGISTERS] = std::array::from_fn(|_| super::scalar::Scalar::None);
								std::hint::black_box(&regs);
							}
							if matches!(scalar, super::scalar::Scalar::Number(v) if v == three) {
								hits += 1;
							}
						}
						Ok(std::ops::ControlFlow::Continue(()))
					})
					.await
					.expect("scan");
				if stats.rows == 0 {
					break;
				}
			}
			println!("{step:<28}{:>7.1} ns/row ({hits})", per_row(started));
		}

		let bound = bind(&ctx, "SELECT VALUE i FROM t WHERE n = 3").await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("covered");
		let started = web_time::Instant::now();
		let out = outcome(&vm, &ctx).await.expect("runs");
		println!("vm, n = 3        {:>7.1} ns/row ({} out)", per_row(started), out.len());
	}
}

/// A table's computed fields and field guards are applied to its owner and to a
/// record user: the VM runs each statement rather than declining it, and never
/// shows a field a guard hides.
#[tokio::test]
async fn a_projection_layer_derives_and_hides_fields() {
	let mut golden = Golden::new("a_projection_layer_derives_and_hides_fields");
	let db = TestDb::new_with_auth(
		"
		DEFINE TABLE bag PERMISSIONS FULL;
		DEFINE FIELD arr ON bag TYPE array;
		DEFINE FIELD hidden ON bag PERMISSIONS FOR select WHERE $auth.id = user:x;
		DEFINE FIELD veiled ON bag PERMISSIONS FOR select WHERE open > 1;
		CREATE bag:1 SET arr = [{ secret: 's1', shown: 1 }], hidden = 'h', veiled = 'v1', open = 1;
		CREATE bag:2 SET arr = [], hidden = 'h', veiled = 'v2', open = 2;
		DEFINE TABLE c SCHEMAFULL PERMISSIONS FOR select WHERE age > 1;
		DEFINE FIELD age ON c TYPE int;
		DEFINE FIELD d ON c COMPUTED age * 2;
		DEFINE FIELD e ON c COMPUTED d + 1 PERMISSIONS FOR select WHERE age > 5;
		CREATE c:1 SET age = 1;
		CREATE c:2 SET age = 3;
		CREATE c:3 SET age = 7;
	",
	)
	.await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, ctx) in [
		("owner", db.exec_ctx().await),
		("record user", db.exec_ctx_as(&reader, crate::kvs::TransactionType::Read).await),
	] {
		for sql in [
			"SELECT * FROM bag",
			"SELECT arr, open FROM bag WHERE open > 1",
			"SELECT hidden FROM bag WHERE hidden",
			"SELECT VALUE hidden FROM bag",
			"SELECT veiled FROM bag",
			"SELECT * FROM c",
			"SELECT * FROM c WHERE d > 2",
			"SELECT e FROM c ORDER BY e DESC",
			"SELECT VALUE d FROM c LIMIT 1 START 1",
			"SELECT math::sum(e) AS s FROM c GROUP ALL",
			"SELECT * FROM c:1..3",
			// A predicate that reads nothing a layer derives sits beneath it.
			"SELECT * FROM c WHERE age > 2",
			"SELECT d FROM c WHERE age > 2 AND d > 2",
			"SELECT * FROM c WHERE age = 3 LIMIT 1",
			"SELECT * FROM c WHERE age > 1 ORDER BY d DESC",
			"SELECT * FROM c WHERE age > 1 SPLIT d",
			"SELECT veiled FROM bag WHERE open = 2",
			"SELECT * FROM bag WHERE open > 0 LIMIT 1",
		] {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}` for the {who}"));
			let expected = outcome(&vm, &ctx).await;
			golden.rows(format!("{who}: {sql}"), &expected);
			let physical = super::optimize::optimize(
				super::map::map(&planner(&ctx), &bound).await.expect("maps"),
			);
			let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
			let layered = program
				.pipelines
				.iter()
				.flat_map(|p| &p.code)
				.any(|i| matches!(i, super::program::Instr::LandRow { .. }));
			// `c` derives fields for everyone; `bag` only guards, and only
			// the fields a statement reads.
			let guarded = ["*", "hidden", "veiled"].iter().any(|f| sql.contains(f));
			let wants_layer = sql.contains("FROM c") || (who != "owner" && guarded);
			assert_eq!(layered, wants_layer, "`{sql}` for the {who}");
			let shown = format!("{expected:?}");
			assert!(
				who == "owner" || !(shown.contains("\"h\"") || shown.contains("v1")),
				"`{sql}` shows the {who} {shown}"
			);
		}
	}
	golden.check();
}

/// A computed sort key naming a projected field's alias is evaluated once per
/// row, for the sort, and the field returns that value: a key whose evaluation
/// is observable — it advances a sequence, directly or through a record's
/// computed field — draws once per row. Each statement runs in a datastore of
/// its own, since the sequence is state.
#[tokio::test]
async fn a_sort_key_a_field_names_is_evaluated_once() {
	const SETUP: &str = "
		DEFINE SEQUENCE seq;
		DEFINE TABLE t SCHEMALESS;
		CREATE t:1 SET v = 3;
		CREATE t:2 SET v = 1;
		CREATE t:3 SET v = 2;
		DEFINE TABLE drawn SCHEMALESS;
		DEFINE FIELD c ON drawn COMPUTED sequence::nextval('seq');
		CREATE drawn:1 SET w = 1;
		CREATE link:1 SET to = drawn:1, v = 2;
		CREATE link:2 SET to = drawn:1, v = 1;
	";
	let mut golden = Golden::new("a_sort_key_a_field_names_is_evaluated_once");
	for sql in [
		"SELECT sequence::nextval('seq') AS n FROM t ORDER BY n",
		"SELECT sequence::nextval('seq') AS n, v FROM t ORDER BY n DESC",
		"SELECT *, sequence::nextval('seq') AS n FROM t ORDER BY n DESC",
		"SELECT sequence::nextval('seq') AS a, sequence::nextval('seq') AS b FROM t ORDER BY b",
		"SELECT VALUE sequence::nextval('seq') AS n FROM t ORDER BY n",
		"SELECT to.c AS x, v FROM link ORDER BY x",
		"SELECT to.c AS x, v FROM link ORDER BY x DESC LIMIT 1",
		"SELECT v + sequence::nextval('seq') AS s FROM t ORDER BY s, v",
	] {
		let db = TestDb::new(SETUP).await;
		let ctx = db.exec_ctx().await;
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		golden.rows(sql, &outcome(&op, &ctx).await);
	}
	golden.check();
}

/// A zero limit reads no record from storage — a table, a record, a range,
/// a union of them, whatever sorts or groups them; `START` alone judges the
/// rows it skips. A range or a union stops at its limit whatever its body.
#[tokio::test]
async fn a_zero_limit_reads_no_record_from_storage() {
	let mut golden = Golden::new("a_zero_limit_reads_no_record_from_storage");
	let db = TestDb::new(
		"
		DEFINE TABLE t SCHEMALESS;
		DEFINE FIELD bad ON t COMPUTED <int> 'x';
		CREATE t:1 SET a = 1 RETURN NONE;
		CREATE t:2 SET a = 1 RETURN NONE;
		DEFINE TABLE u SCHEMALESS;
		CREATE u:1 SET a = 1 RETURN NONE;
	",
	)
	.await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT * FROM t LIMIT 0",
		"SELECT * FROM t:1 LIMIT 0",
		"SELECT * FROM t:1 LIMIT 0 START 1",
		"SELECT * FROM t:1 START 1",
		"SELECT * FROM u:1 LIMIT 0",
		"SELECT * FROM u:1..3 LIMIT 0",
		"SELECT * FROM u:1..3 WHERE a = 1 LIMIT 1",
		"SELECT * FROM u, u:1 LIMIT 0",
		"SELECT * FROM u, u LIMIT 1",
		"SELECT * FROM t START 5",
		"SELECT * FROM u WHERE <int> 'x' LIMIT 0",
		"SELECT * FROM u:1 WHERE <int> 'x' LIMIT 0",
		"SELECT * FROM u WHERE <int> 'x' ORDER BY a LIMIT 0",
		"SELECT count() FROM u WHERE <int> 'x' GROUP ALL LIMIT 0",
		"SELECT a FROM u WHERE <int> 'x' GROUP BY a LIMIT 0",
		"SELECT * FROM t WHERE a = 1 SPLIT a LIMIT 0",
		"SELECT * FROM array::at(array::shuffle([t:1]), 0) LIMIT 0",
		"SELECT * FROM [1] WHERE <int> 'x' LIMIT 0",
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		golden.rows(sql, &outcome(&vm, &ctx).await);
	}
	golden.check();
	// A range or a union stops at its limit as a scan does, whatever its
	// body: a row past the limit is neither read nor judged.
	for (sql, fails) in [
		("SELECT * FROM t:1..3 LIMIT 0", false),
		("SELECT * FROM u:1, u:2 WHERE <int> 'x' LIMIT 0", false),
		("SELECT * FROM t, u LIMIT 1", true),
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		let outcome = outcome(&vm, &ctx).await;
		assert_eq!(outcome.is_err(), fails, "`{sql}` answered {outcome:?}");
		if !fails {
			assert_eq!(outcome, Ok(Vec::new()), "`{sql}`");
		}
	}
}

/// A LIMIT over a record range reads up to its limit, in the read's own
/// batching, and stops: a range of thousands of records whose computed field
/// fails on the last reads about one batch for `LIMIT 1`, and answers.
#[tokio::test]
async fn a_limited_range_reads_about_one_batch() {
	let db = TestDb::new(
		"
		DEFINE TABLE t SCHEMALESS;
		DEFINE FIELD c ON t COMPUTED IF a = 3000 { <int> 'x' } ELSE { a };
		FOR $i IN 1..=3000 { CREATE type::record('t', $i) SET a = $i RETURN NONE; };
	",
	)
	.await;
	let ctx = db.exec_ctx().await;
	let sql = "SELECT VALUE c FROM t:1.. LIMIT 1";
	let bound = bind(&ctx, sql).await;
	let vm = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	super::host::RECORDS_SCANNED.with(|scanned| scanned.set(0));
	assert_eq!(outcome(&vm, &ctx).await, Ok(vec![Value::from(1i64)]), "`{sql}`");
	let read = super::host::RECORDS_SCANNED.with(std::cell::Cell::get);
	assert!((1..=500).contains(&read), "`{sql}` read {read} records");
}

/// Row-at-a-time against vectorized evaluation of one filter,
/// `string::len(name) > 2 AND n * 2 > 5`, over rows already in memory: the
/// dispatch and evaluation cost alone, with the vectorized form paying for
/// gathering its columns. Run in release, as `throughput`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn row_against_vector() {
	use crate::val::{Number, Object};

	const ROWS: usize = 200_000;
	const WIDTH: usize = 1024;
	let rows: Vec<Value> = (0..ROWS)
		.map(|i| {
			let mut object = Object::default();
			object.insert("name", Value::from("n".repeat(1 + i % 5)));
			object.insert("n", Value::Number(Number::Int((i % 7) as i64)));
			Value::Object(object)
		})
		.collect();

	#[derive(Clone, Copy)]
	enum R<'a> {
		None,
		Int(i64),
		Str(&'a str),
		Bool(bool),
	}
	enum Op {
		LoadField(usize, &'static str),
		StrLen(usize, usize),
		Const(usize, i64),
		Mul(usize, usize, usize),
		Gt(usize, usize, usize),
		JumpIfFalse(usize, usize),
		Emit,
	}
	let program = [
		Op::LoadField(0, "name"),
		Op::StrLen(1, 0),
		Op::Const(2, 2),
		Op::Gt(3, 1, 2),
		Op::JumpIfFalse(3, 10),
		Op::LoadField(4, "n"),
		Op::Const(5, 2),
		Op::Mul(6, 4, 5),
		Op::Const(7, 5),
		Op::Gt(3, 6, 7),
		Op::JumpIfFalse(3, 12),
		Op::Emit,
	];

	let row_mode = |rows: &[Value]| -> usize {
		let mut emitted = 0;
		for row in rows {
			let Value::Object(object) = row else {
				continue;
			};
			let mut regs = [R::None; 8];
			let mut pc = 0;
			while pc < program.len() {
				match &program[pc] {
					Op::LoadField(dst, name) => {
						regs[*dst] = match object.get(name) {
							Some(Value::Number(Number::Int(n))) => R::Int(*n),
							Some(Value::String(s)) => R::Str(s.as_str()),
							_ => R::None,
						};
					}
					Op::StrLen(dst, src) => {
						regs[*dst] = match regs[*src] {
							R::Str(s) => R::Int(s.chars().count() as i64),
							_ => R::None,
						};
					}
					Op::Const(dst, n) => regs[*dst] = R::Int(*n),
					Op::Mul(dst, a, b) => {
						regs[*dst] = match (regs[*a], regs[*b]) {
							(R::Int(a), R::Int(b)) => a.checked_mul(b).map_or(R::None, R::Int),
							_ => R::None,
						};
					}
					Op::Gt(dst, a, b) => {
						regs[*dst] = match (regs[*a], regs[*b]) {
							(R::Int(a), R::Int(b)) => R::Bool(a > b),
							_ => R::Bool(false),
						};
					}
					Op::JumpIfFalse(cond, target) => {
						if !matches!(regs[*cond], R::Bool(true)) {
							pc = *target;
							continue;
						}
					}
					Op::Emit => emitted += 1,
				}
				pc += 1;
			}
		}
		emitted
	};

	let vector_mode = |rows: &[Value]| -> usize {
		let mut emitted = 0;
		let mut names: Vec<&str> = Vec::with_capacity(WIDTH);
		let mut ns: Vec<i64> = Vec::with_capacity(WIDTH);
		let mut selection: Vec<u16> = Vec::with_capacity(WIDTH);
		let mut overflowed = false;
		for chunk in rows.chunks(WIDTH) {
			names.clear();
			ns.clear();
			for row in chunk {
				let Value::Object(object) = row else {
					names.push("");
					ns.push(0);
					continue;
				};
				names.push(match object.get("name") {
					Some(Value::String(s)) => s.as_str(),
					_ => "",
				});
				ns.push(match object.get("n") {
					Some(Value::Number(Number::Int(n))) => *n,
					_ => 0,
				});
			}
			// Left operand over every row, narrowing the selection.
			selection.clear();
			for (i, name) in names.iter().enumerate() {
				if name.chars().count() as i64 > 2 {
					selection.push(i as u16);
				}
			}
			// Right operand only over the rows the left admitted.
			selection.retain(|&i| match ns[i as usize].checked_mul(2) {
				Some(v) => v > 5,
				None => {
					overflowed = true;
					false
				}
			});
			emitted += selection.len();
		}
		assert!(!overflowed);
		emitted
	};

	// The same work split: gathering the columns once, then evaluating over
	// them, and the row interpreter over rows whose fields are already in
	// registers, so each side's evaluation is timed without the gather.
	let columns: Vec<(Vec<&str>, Vec<i64>)> = rows
		.chunks(WIDTH)
		.map(|chunk| {
			chunk
				.iter()
				.map(|row| match row {
					Value::Object(object) => (
						match object.get("name") {
							Some(Value::String(s)) => s.as_str(),
							_ => "",
						},
						match object.get("n") {
							Some(Value::Number(Number::Int(n))) => *n,
							_ => 0,
						},
					),
					_ => ("", 0),
				})
				.unzip()
		})
		.collect();
	let vector_compute = |columns: &[(Vec<&str>, Vec<i64>)]| -> usize {
		let mut emitted = 0;
		let mut selection: Vec<u16> = Vec::with_capacity(WIDTH);
		for (names, ns) in columns {
			selection.clear();
			for (i, name) in names.iter().enumerate() {
				if name.chars().count() as i64 > 2 {
					selection.push(i as u16);
				}
			}
			selection.retain(|&i| ns[i as usize].checked_mul(2).is_some_and(|v| v > 5));
			emitted += selection.len();
		}
		emitted
	};
	let row_compute = |columns: &[(Vec<&str>, Vec<i64>)]| -> usize {
		let mut emitted = 0;
		for (names, ns) in columns {
			for (name, n) in names.iter().zip(ns) {
				let mut regs = [R::None; 8];
				let mut pc = 0;
				while pc < program.len() {
					match &program[pc] {
						Op::LoadField(dst, field) => {
							regs[*dst] = if *field == "name" {
								R::Str(name)
							} else {
								R::Int(*n)
							};
						}
						Op::StrLen(dst, src) => {
							regs[*dst] = match regs[*src] {
								R::Str(s) => R::Int(s.chars().count() as i64),
								_ => R::None,
							};
						}
						Op::Const(dst, v) => regs[*dst] = R::Int(*v),
						Op::Mul(dst, a, b) => {
							regs[*dst] = match (regs[*a], regs[*b]) {
								(R::Int(a), R::Int(b)) => a.checked_mul(b).map_or(R::None, R::Int),
								_ => R::None,
							};
						}
						Op::Gt(dst, a, b) => {
							regs[*dst] = match (regs[*a], regs[*b]) {
								(R::Int(a), R::Int(b)) => R::Bool(a > b),
								_ => R::Bool(false),
							};
						}
						Op::JumpIfFalse(cond, target) => {
							if !matches!(regs[*cond], R::Bool(true)) {
								pc = *target;
								continue;
							}
						}
						Op::Emit => emitted += 1,
					}
					pc += 1;
				}
			}
		}
		emitted
	};

	for _ in 0..3 {
		let started = web_time::Instant::now();
		let a = row_compute(&columns);
		let row = started.elapsed().as_nanos() as f64 / ROWS as f64;
		let started = web_time::Instant::now();
		let b = vector_compute(&columns);
		let vector = started.elapsed().as_nanos() as f64 / ROWS as f64;
		assert_eq!(a, b);
		println!("evaluation only: row {row:>6.1} ns/row   vector {vector:>6.1} ns/row");
	}

	for _ in 0..3 {
		let started = web_time::Instant::now();
		let a = row_mode(&rows);
		let row = started.elapsed().as_nanos() as f64 / ROWS as f64;
		let started = web_time::Instant::now();
		let b = vector_mode(&rows);
		let vector = started.elapsed().as_nanos() as f64 / ROWS as f64;
		assert_eq!(a, b);
		println!("row {row:>6.1} ns/row   vector {vector:>6.1} ns/row   ({a} rows)");
	}
}

/// One record read per key against one batched read per vector of keys, at
/// the record layer, on the memory engine. The memory engine has no round
/// trip to save, so this measures per-call overhead only.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn point_reads_against_batched_reads() {
	use crate::catalog::providers::TableProvider;
	use crate::kvs::CachePolicy;
	use crate::val::{RecordId, RecordIdKey, TableName};

	const ROWS: usize = 50_000;
	const WIDTH: usize = 1024;
	let db =
		TestDb::new(&format!("FOR $i IN 1..={ROWS} {{ CREATE type::record('u', $i) SET v = $i }}"))
			.await;
	let ctx = db.exec_ctx().await;
	let db_ctx = ctx.database().expect("a database");
	let (ns, dbid) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let txn = ctx.txn();
	let table = TableName::from("u");
	let rids: Vec<RecordId> = (1..=ROWS as i64)
		.map(|i| RecordId {
			table: table.clone(),
			key: RecordIdKey::Number(i),
		})
		.collect();
	for _ in 0..3 {
		let started = web_time::Instant::now();
		let mut found = 0;
		for rid in &rids {
			let record = txn.get_record(ns, dbid, &rid.table, &rid.key, None).await.expect("reads");
			found += usize::from(!record.data.is_none());
		}
		let point = started.elapsed().as_nanos() as f64 / ROWS as f64;
		let started = web_time::Instant::now();
		let mut batched = 0;
		for chunk in rids.chunks(WIDTH) {
			let records =
				txn.get_records(ns, dbid, chunk, None, CachePolicy::ReadOnly).await.expect("reads");
			batched += records.iter().filter(|r| !r.data.is_none()).count();
		}
		let batch = started.elapsed().as_nanos() as f64 / ROWS as f64;
		assert_eq!(found, batched);
		println!("point {point:>7.1} ns/read   batched {batch:>7.1} ns/read   ({found} records)");
	}
}

/// The strict row layout against the `revision` encoding, over the same
/// records held as stored bytes in memory, so the storage cursor's own cost
/// is left out: reading an integer field, string equality and ordering, and
/// the vectorized gather and filter of `row_against_vector`. Run in release,
/// as `throughput`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn strict_layout_against_revision() {
	use std::borrow::Cow;

	use super::layout::{ColumnKind, Layout, StrictRow, StringEncoding};
	use super::row::{FieldName, RevisionRow, RowReader};
	use super::scalar::Scalar;
	use crate::exec::operators::scan::pipeline::decode_record;
	use crate::key::schema::RecordPrefix;

	const ROWS: usize = 200_000;
	const WIDTH: usize = 1024;
	let db = TestDb::new(&format!(
		"FOR $i IN 1..={ROWS} {{ CREATE type::record('t', $i) SET n = $i % 7, i = $i, \
		 name = IF $i % 10 = 0 THEN 'a name long enough to leave its slot' ELSE \
		 string::repeat('n', 1 + $i % 5) END, s = 'row' }}"
	))
	.await;
	let ctx = db.exec_ctx().await;
	let db_ctx = ctx.database().expect("database");
	let range = RecordPrefix {
		ns: db_ctx.ns_ctx.ns.namespace_id,
		db: db_ctx.db.database_id,
		tb: Cow::Borrowed(&"t".into()),
	}
	.range()
	.expect("range");
	let txn = ctx.txn();
	let mut cursor = txn
		.open_vals_cursor_raw(range, crate::kvs::Direction::Forward, 0, None)
		.await
		.expect("cursor");
	let mut stored: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(ROWS);
	loop {
		let stats = cursor
			.for_each(1000, &mut |key, val| {
				stored.push((key.to_vec(), val.to_vec()));
				Ok(std::ops::ControlFlow::Continue(()))
			})
			.await
			.expect("scan");
		if stats.rows == 0 {
			break;
		}
	}
	assert_eq!(stored.len(), ROWS);
	let records: Vec<crate::val::Object> = stored
		.iter()
		.map(|(k, v)| match decode_record(k, v).expect("decodes") {
			Value::Object(object) => object,
			other => panic!("not an object: {other:?}"),
		})
		.collect();

	let per_row = |started: web_time::Instant| started.elapsed().as_nanos() as f64 / ROWS as f64;
	let n = FieldName::new("n");
	let name = FieldName::new("name");

	// The `revision` encoding, read in place as the VM reads it today.
	let revision = |label: &str| {
		let mut hits = 0usize;
		let started = web_time::Instant::now();
		for (k, v) in &stored {
			hits += usize::from(RevisionRow::new(k, v).field_int(&n).expect("reads") == Some(3));
		}
		let int = per_row(started);
		let started = web_time::Instant::now();
		for (k, v) in &stored {
			if let Scalar::Str(s) = RevisionRow::new(k, v).field(&name).expect("reads") {
				hits += usize::from(s == "nnn");
			}
		}
		let eq = per_row(started);
		let started = web_time::Instant::now();
		for (k, v) in &stored {
			if let Scalar::Str(s) = RevisionRow::new(k, v).field(&name).expect("reads") {
				hits += usize::from(s > "nn");
			}
		}
		let cmp = per_row(started);
		let started = web_time::Instant::now();
		for (k, v) in &stored {
			if let Scalar::Str(s) = RevisionRow::new(k, v).field(&name).expect("reads") {
				hits += usize::from(s.chars().count() > 2);
			}
		}
		let len = per_row(started);
		let started = web_time::Instant::now();
		let mut emitted = 0usize;
		let (mut names, mut ns): (Vec<usize>, Vec<i64>) = (Vec::new(), Vec::new());
		for chunk in stored.chunks(WIDTH) {
			names.clear();
			ns.clear();
			for (k, v) in chunk {
				let row = RevisionRow::new(k, v);
				names.push(match row.field(&name).expect("reads") {
					Scalar::Str(s) => s.chars().count(),
					_ => 0,
				});
				ns.push(row.field_int(&n).expect("reads").unwrap_or(0));
			}
			emitted += names
				.iter()
				.zip(&ns)
				.filter(|(len, n)| **len > 2 && n.checked_mul(2).is_some_and(|v| v > 5))
				.count();
		}
		let gather = per_row(started);
		println!(
			"{label:<22} int {int:>6.1}   eq {eq:>6.1}   cmp {cmp:>6.1}   len>2 {len:>6.1}   gather+filter {gather:>6.1} ns/row   ({hits} {emitted})"
		);
	};

	let strict = |label: &str, strings: StringEncoding| {
		let layout = Layout::plan(
			1,
			&[
				("i", ColumnKind::Int),
				("n", ColumnKind::Int),
				("name", ColumnKind::String),
				("s", ColumnKind::String),
			],
			strings,
		)
		.expect("plans");
		let rows: Vec<Vec<u8>> =
			records.iter().map(|r| layout.encode(r).expect("encodes")).collect();
		let bytes: usize = rows.iter().map(Vec::len).sum();
		let (n_col, name_col) =
			(layout.column("n").expect("n"), layout.column("name").expect("name"));
		let mut hits = 0usize;
		let started = web_time::Instant::now();
		for row in &rows {
			hits += usize::from(StrictRow::new(&layout, row).read_int(n_col) == Some(3));
		}
		let int = per_row(started);
		let started = web_time::Instant::now();
		for row in &rows {
			hits += usize::from(StrictRow::new(&layout, row).str_eq(name_col, "nnn"));
		}
		let eq = per_row(started);
		let started = web_time::Instant::now();
		for row in &rows {
			hits += usize::from(
				StrictRow::new(&layout, row).str_cmp(name_col, "nn")
					== Some(std::cmp::Ordering::Greater),
			);
		}
		let cmp = per_row(started);
		let started = web_time::Instant::now();
		for row in &rows {
			hits +=
				usize::from(StrictRow::new(&layout, row).str_len(name_col).is_some_and(|n| n > 2));
		}
		let len = per_row(started);
		let started = web_time::Instant::now();
		let mut emitted = 0usize;
		let (mut names, mut ns): (Vec<usize>, Vec<i64>) = (Vec::new(), Vec::new());
		for chunk in rows.chunks(WIDTH) {
			names.clear();
			ns.clear();
			for row in chunk {
				let row = StrictRow::new(&layout, row);
				names.push(match row.read(name_col) {
					Scalar::Str(s) => s.chars().count(),
					_ => 0,
				});
				ns.push(row.read_int(n_col).unwrap_or(0));
			}
			emitted += names
				.iter()
				.zip(&ns)
				.filter(|(len, n)| **len > 2 && n.checked_mul(2).is_some_and(|v| v > 5))
				.count();
		}
		let gather = per_row(started);
		println!(
			"{label:<22} int {int:>6.1}   eq {eq:>6.1}   cmp {cmp:>6.1}   len>2 {len:>6.1}   gather+filter {gather:>6.1} ns/row   ({hits} {emitted}, {:.0} B/row)",
			bytes as f64 / ROWS as f64
		);
	};

	let revision_bytes: usize = stored.iter().map(|(_, v)| v.len()).sum();
	println!("revision value size {:.0} B/row", revision_bytes as f64 / ROWS as f64);
	for _ in 0..2 {
		revision("revision");
		strict("strict offset table", StringEncoding::OffsetTable);
		strict(
			"strict view 12",
			StringEncoding::View {
				inline: 12,
			},
		);
		strict(
			"strict view 8 (stats)",
			StringEncoding::View {
				inline: 8,
			},
		);
	}
}

/// Why the VM declines `sql`, bound by `ctx`, or `None` where it lowers it
/// to a program.
pub(super) async fn decline_of(ctx: &ExecutionContext, sql: &str) -> Option<String> {
	let bound = bind(ctx, sql).await;
	let planner = planner(ctx);
	let mapped = match super::map::map(&planner, &bound).await {
		Ok(mapped) => mapped,
		Err(declined) => return Some(declined.reason),
	};
	let physical = super::optimize::optimize(mapped);
	super::lower::lower(&planner, &physical).await.err().map(|declined| declined.reason)
}

/// A recursion through nested statements runs on the statement's frame
/// stack: a function that calls itself from inside a block runs as deep on a
/// thread whose stack holds only a few levels of it in place.
#[test]
fn nested_statements_hold_the_native_stack() {
	let run = std::thread::Builder::new()
		.stack_size(4 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build();
			runtime.expect("a runtime").block_on(async {
				let db = TestDb::new(
					"DEFINE FUNCTION fn::down($d: int) { \
					 IF $d > 0 { RETURN fn::down($d - 1) + 1 } ELSE { RETURN 0 } };",
				)
				.await;
				let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
				let bound = bind(&ctx, "fn::down(100)").await;
				let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers it");
				outcome(&op, &ctx).await
			})
		})
		.expect("a thread");
	assert_eq!(run.join().expect("no overflow"), Ok(vec![Value::from(100)]));
}

/// A function whose body is one expression over its arguments is inlined:
/// the call binds its arguments into registers and runs the body's
/// instructions in the caller's program. A body that reads data, or runs
/// statements, is called.
#[tokio::test]
async fn an_expression_body_is_inlined() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let inlines = |sql: &'static str| {
		let ctx = &ctx;
		async move {
			let bound = bind(ctx, sql).await;
			let planner = planner(ctx);
			let physical =
				super::optimize::optimize(super::map::map(&planner, &bound).await.unwrap());
			let program = super::lower::lower(&planner, &physical).await.unwrap();
			program.pipelines.iter().any(|pipeline| {
				pipeline
					.code
					.iter()
					.any(|instr| matches!(instr, super::program::Instr::FunctionBind { .. }))
			})
		}
	};
	assert!(inlines("SELECT fn::inc(age) AS a FROM person").await);
	assert!(inlines("fn::mix(2, 3)").await);
	assert!(inlines("fn::pick($limit)").await);
	assert!(!inlines("fn::oldest()").await, "a body that reads data is called");
	assert!(!inlines("fn::fact(3)").await, "a body of statements is called");
	assert!(!inlines("fn::named({ name: 'x' })").await, "a body that reads a field is called");
}

/// A call whose function is redefined after the plan inlined its body fails
/// closed: it finds a definition that hashes otherwise than the one the body
/// was lowered from, and runs the body of neither.
#[tokio::test]
async fn an_inlined_body_stands_only_for_its_definition() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, "fn::mix(2, 3)").await;
	let call = super::plan(&planner(&ctx), &bound).await.expect("the VM covers it");
	assert_eq!(outcome(&call, &ctx).await, Ok(vec![Value::from(7)]));
	let redefine =
		bind(&ctx, "DEFINE FUNCTION OVERWRITE fn::mix($a: int, $b: int) { $a - $b }").await;
	let redefine = super::plan(&planner(&ctx), &redefine).await.expect("the VM plans it");
	outcome(&redefine, &ctx).await.expect("redefined");
	let error = outcome(&call, &ctx).await.expect_err("a call of a definition it was not lowered");
	assert!(error.contains("redefined after the program that calls it was lowered"), "{error}");
}

/// The VM executes REBUILD INDEX and validates ES512 access methods.
#[tokio::test]
async fn the_vm_executes_extended_ddl() {
	let db =
		TestDb::new("DEFINE TABLE t; DEFINE INDEX idx ON t FIELDS n; CREATE t:1 SET n = 1;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;

	// REBUILD INDEX
	let bound = bind(&ctx, "REBUILD INDEX idx ON t").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers REBUILD INDEX");
	assert_eq!(outcome(&op, &ctx).await, Ok(vec![Value::None]));

	// REBUILD INDEX IF EXISTS on non-existent index
	let bound = bind(&ctx, "REBUILD INDEX IF EXISTS missing ON t").await;
	let op =
		super::plan(&planner(&ctx), &bound).await.expect("the VM covers REBUILD INDEX IF EXISTS");
	assert_eq!(outcome(&op, &ctx).await, Ok(vec![Value::None]));

	// ES512 rejected on new definition
	let bound =
		bind(&ctx, "DEFINE ACCESS test ON DATABASE TYPE JWT ALGORITHM ES512 KEY 'secret'").await;
	let op =
		super::plan(&planner(&ctx), &bound).await.expect("the VM covers DEFINE ACCESS with ES512");
	assert!(outcome(&op, &ctx).await.is_err());
}

/// The VM executes FullTextScan and KnnScan natively.
#[tokio::test]
async fn the_vm_executes_fts_and_knn() {
	let db = TestDb::new(
		"
		DEFINE TABLE book SCHEMALESS;
		DEFINE ANALYZER standard TOKENIZERS blank,class FILTERS lowercase,ascii;
		DEFINE INDEX title_ft ON book FIELDS title FULLTEXT ANALYZER standard;
		CREATE book:1 SET title = 'SurrealDB database guide';
		CREATE book:2 SET title = 'Postgres guide';

		DEFINE TABLE vector_item SCHEMALESS;
		DEFINE INDEX vec_hnsw ON vector_item FIELDS vec HNSW DIMENSION 4 DISTANCE EUCLIDEAN;
		CREATE vector_item:1 SET vec = [1.0, 0.0, 0.0, 0.0];
		CREATE vector_item:2 SET vec = [0.0, 1.0, 0.0, 0.0];
	",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;

	// Full-text query on VM
	let bound = bind(&ctx, "SELECT id, title FROM book WHERE title @1@ 'database'").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers FTS query");
	let res = outcome(&op, &ctx).await.expect("runs on VM");
	assert_eq!(res.len(), 1);

	// KNN query on VM
	let bound =
		bind(&ctx, "SELECT id FROM vector_item WHERE vec <|1, 40|> [0.9, 0.1, 0.0, 0.0]").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers KNN query");
	let res = outcome(&op, &ctx).await.expect("runs on VM");
	assert_eq!(res.len(), 1);
}

/// The VM executes OMIT of deep paths and union sources natively.
#[tokio::test]
async fn the_vm_executes_omit_path_and_unions() {
	let db = TestDb::new(
		"
		DEFINE TABLE person SCHEMALESS;
		CREATE person:1 SET name = 'ann', nested = { x: 1, y: 2 };
	",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;

	// OMIT of deep path
	let bound = bind(&ctx, "SELECT * OMIT nested.x FROM person WHERE id = person:1").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers OMIT of deep path");
	let res = outcome(&op, &ctx).await.expect("runs on VM");
	assert_eq!(res.len(), 1);
	if let Value::Object(obj) = &res[0] {
		if let Some(Value::Object(nested)) = obj.get("nested") {
			assert!(nested.get("x").is_none());
			assert!(nested.get("y").is_some());
		} else {
			panic!("expected nested object");
		}
	} else {
		panic!("expected object");
	}
}

/// The VM executes INFO FOR INDEX statements natively.
#[tokio::test]
async fn the_vm_executes_info_for_index() {
	let db = TestDb::new(
		"
		DEFINE TABLE person SCHEMALESS;
		DEFINE INDEX idx_person_name ON person FIELDS name;
		CREATE person:1 SET name = 'ann';
	",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;

	let bound = bind(&ctx, "INFO FOR INDEX idx_person_name ON person").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers INFO FOR INDEX");
	let res = outcome(&op, &ctx).await.expect("runs on VM");
	assert_eq!(res.len(), 1);
	if let Value::Object(obj) = &res[0] {
		assert!(obj.get("building").is_some());
	} else {
		panic!("expected object");
	}
}

/// The VM executes EXPLAIN and EXPLAIN ANALYZE statements natively.
#[tokio::test]
async fn the_vm_executes_explain_and_analyze() {
	let db = TestDb::new(
		"
		DEFINE TABLE person SCHEMALESS;
		CREATE person:1 SET name = 'ann';
		CREATE person:2 SET name = 'bob';
	",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;

	// EXPLAIN text
	let bound = bind(&ctx, "EXPLAIN SELECT * FROM person").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers EXPLAIN");
	let res = outcome(&op, &ctx).await.expect("runs on VM");
	assert_eq!(res.len(), 1);
	let Value::String(s) = &res[0] else {
		panic!("expected string");
	};
	assert!(s.contains("SelectProject"));
	assert!(s.contains("TableScan"));

	// EXPLAIN ANALYZE text
	let bound = bind(&ctx, "EXPLAIN ANALYZE SELECT * FROM person").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers EXPLAIN ANALYZE");
	let res = outcome(&op, &ctx).await.expect("runs on VM");
	assert_eq!(res.len(), 1);
	let Value::String(s) = &res[0] else {
		panic!("expected string");
	};
	assert!(s.contains("Total rows: 2"));

	// EXPLAIN ANALYZE FORMAT JSON
	let bound = bind(&ctx, "EXPLAIN ANALYZE FORMAT JSON SELECT * FROM person").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers EXPLAIN ANALYZE JSON");
	let res = outcome(&op, &ctx).await.expect("runs on VM");
	assert_eq!(res.len(), 1);
	let Value::Object(obj) = &res[0] else {
		panic!("expected object");
	};
	assert_eq!(obj.get("total_rows"), Some(&Value::from(2)));
}

/// The VM executes scans over lightweight relations natively.
#[tokio::test]
async fn the_vm_executes_lightweight_relation_scans() {
	let db = TestDb::new(
		"
		DEFINE TABLE person SCHEMALESS;
		DEFINE TABLE pal TYPE RELATION IN person OUT person LIGHTWEIGHT;
		CREATE person:ann;
		CREATE person:bob;
		RELATE person:ann->pal->person:bob;
	",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;

	let bound = bind(&ctx, "SELECT * FROM pal").await;
	let op =
		super::plan(&planner(&ctx), &bound).await.expect("the VM covers lightweight relation scan");
	let res = outcome(&op, &ctx).await.expect("runs on VM");
	assert_eq!(res.len(), 1);
	if let Value::Object(obj) = &res[0] {
		assert_eq!(
			obj.get("in"),
			Some(&Value::from(crate::val::RecordId::new("person".into(), "ann".to_string())))
		);
		assert_eq!(
			obj.get("out"),
			Some(&Value::from(crate::val::RecordId::new("person".into(), "bob".to_string())))
		);
	} else {
		panic!("expected object");
	}
}

/// The VM executes TopK pushdown for ORDER BY ... LIMIT.
#[tokio::test]
async fn the_vm_executes_topk_pushdown() {
	let db = TestDb::new(
		"
		DEFINE TABLE item SCHEMALESS;
		CREATE item:1 SET score = 10;
		CREATE item:2 SET score = 50;
		CREATE item:3 SET score = 30;
		CREATE item:4 SET score = 20;
		CREATE item:5 SET score = 40;
	",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;

	let bound = bind(&ctx, "SELECT score FROM item ORDER BY score DESC LIMIT 2").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("covered");
	let res = outcome(&op, &ctx).await.expect("runs");
	assert_eq!(res.len(), 2);
	if let (Value::Object(r1), Value::Object(r2)) = (&res[0], &res[1]) {
		assert_eq!(r1.get("score"), Some(&Value::from(50)));
		assert_eq!(r2.get("score"), Some(&Value::from(40)));
	} else {
		panic!("expected objects");
	}
}
