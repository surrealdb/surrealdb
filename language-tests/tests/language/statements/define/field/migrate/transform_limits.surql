/**
[test]
reason = "A transform reads `$value` alone: a step that would fetch the record a link names, a method an object may answer with a closure, a user-defined function and a FALLBACK that is not a literal are refused when the migration is defined, and a step through a value whose type admits no link is accepted."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, name: 'Ann' }]"

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
value = "[{ a: person:1, id: t:1, l: [person:1], o: { name: 'Bo', next: person:1 }, p: { link: person:1, name: 'Cy' } }]"

[[test.results]]
error = "Cannot migrate field `a` of table `t`: the transform may not read a linked record, and `$value.name` reads through a value that may be a record link"

[[test.results]]
error = "Cannot migrate field `a` of table `t`: the transform may not read a linked record, and `$value.*` reads through a value that may be a record link"

[[test.results]]
error = "Cannot migrate field `a` of table `t`: the transform may not read a linked record, and `$value.{ name }` reads through a value that may be a record link"

[[test.results]]
error = "Cannot migrate field `l` of table `t`: the transform may not read a linked record, and `$value.name` reads through a value that may be a record link"

[[test.results]]
error = "Cannot migrate field `l` of table `t`: the transform may not read a linked record, and `$v.name` reads through a value that may be a record link"

[[test.results]]
error = "Cannot migrate field `a` of table `t`: the transform may not read a linked record, and `type::record('person', 1).name` reads through a value that may be a record link"

[[test.results]]
error = "Cannot migrate field `o` of table `t`: the transform may not read a linked record, and `(<object> $value).next.name` reads through a value that may be a record link"

[[test.results]]
error = "Cannot migrate field `o` of table `t`: the transform may not read a linked record, and `(<object> $value).{ next.* }` reads through a value that may be a record link"

[[test.results]]
error = "Cannot migrate field `o` of table `t`: the transform may not read a linked record, and `$value.name` reads through a value that may be a record link"

[[test.results]]
error = "Cannot migrate field `l` of table `t`: the transform may not call `.map()` as a method, which an object answers with a closure it holds: call the builtin function instead"

[[test.results]]
error = "Cannot migrate field `a` of table `t`: the transform may not call a user-defined function, a model, a script or a module"

[[test.results]]
error = "An error occurred: A migration's FALLBACK must be a constant"

[[test.results]]
error = "An error occurred: A migration's FALLBACK must be a constant"

[[test.results]]
error = "An error occurred: A migration's FALLBACK must be a constant"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: A migration's FALLBACK must be a constant"

[[test.results]]
value = "{ a: person:1, l: [person:1], o: { name: 'Bo', next: person:1 }, p: { link: person:1, name: 'Cy' } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate field `p` of table `t`: the transform may not read a linked record, and `$value.name` reads through a value that may be a record link"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: '1', l: ['1'], o: 'Bo', p: { link: person:1, name: 'Cy' } }"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
CREATE person:1 SET name = 'Ann';
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD a ON t TYPE record<person>;
DEFINE FIELD l ON t TYPE array<record<person>>;
DEFINE FIELD o ON t TYPE object FLEXIBLE;
DEFINE FIELD p ON t TYPE { name: string, link: record<person> };
DEFINE FUNCTION fn::a($v: any) { RETURN <string>$v };
CREATE t:1 SET a = person:1, l = [person:1], o = { name: 'Bo', next: person:1 }, p = { name: 'Cy', link: person:1 };
-- A field read through a link reads the linked record.
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS d1 USING $value.name FALLBACK '';
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS d2 USING <string>$value.* FALLBACK '';
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS d3 USING <string>$value.{ name } FALLBACK '';
ALTER FIELD l ON t TYPE array<string> MIGRATE EXISTING ROWS AS d4 USING $value.name FALLBACK [];
ALTER FIELD l ON t TYPE array<string> MIGRATE EXISTING ROWS AS d5 USING array::map($value, |$v| $v.name) FALLBACK [];
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS d6 USING type::record('person', 1).name FALLBACK '';
-- An untyped object's field may hold a link.
ALTER FIELD o ON t DROP FLEXIBLE TYPE string MIGRATE EXISTING ROWS AS d7 USING <string>(<object>$value).next.name FALLBACK '';
ALTER FIELD o ON t DROP FLEXIBLE TYPE string MIGRATE EXISTING ROWS AS d8 USING <string>(<object>$value).{ next.* } FALLBACK '';
-- A field defined without a migration promises nothing of the rows stored
-- before it, so its value may be a link whatever its type says.
ALTER FIELD o ON t DROP FLEXIBLE TYPE string MIGRATE EXISTING ROWS AS d9 USING $value.name FALLBACK '';
-- A method an object may answer with a closure it holds is not a builtin.
ALTER FIELD l ON t TYPE array<string> MIGRATE EXISTING ROWS AS m1 USING $value.map(|$v: record<person>| <string>record::id($v)) FALLBACK [];
-- A user-defined function's body is not the transform's.
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS f1 USING fn::a($value) FALLBACK '';
-- A FALLBACK is a literal.
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS c1 USING <string>$value FALLBACK fn::a(1);
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS c2 USING <string>$value FALLBACK <string>time::now();
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS c3 USING <string>$value FALLBACK (SELECT VALUE name FROM ONLY person:1);
LET $p = 'x';
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS c4 USING <string>$value FALLBACK $p;
-- Nothing was migrated.
SELECT a, l, o, p FROM ONLY t:1;
-- A link's own parts are read without fetching it; a field read through a
-- value cast to a type that admits no link, or of a migrated field whose type
-- admits none, is a plain read.
ALTER FIELD a ON t TYPE string MIGRATE EXISTING ROWS AS ok1 USING <string>record::id($value) FALLBACK '';
ALTER FIELD o ON t DROP FLEXIBLE TYPE string MIGRATE EXISTING ROWS AS ok2 USING (<object>$value).name FALLBACK '';
ALTER FIELD p ON t MIGRATE EXISTING ROWS AS ok3 USING $value FALLBACK { name: '', link: person:1 };
ALTER FIELD p ON t TYPE string MIGRATE EXISTING ROWS AS ok4 USING $value.name + ':' + <string>record::id($value.link) FALLBACK '';
ALTER FIELD l ON t TYPE array<string> MIGRATE EXISTING ROWS AS ok5 USING array::map($value, |$v: record<person>| <string>record::id($v)) FALLBACK [];
SELECT a, l, o, p FROM ONLY t:1;
