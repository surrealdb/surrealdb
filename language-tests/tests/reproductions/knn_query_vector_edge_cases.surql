/**
[env]
namespace = true
database = true

[test]
reason = """
The edge cases of a KNN query vector, for an HNSW index (`pts`, `<|k,EF|>`)
and for brute force (`bpts`, `<|k,DIST|>`), known at planning (a literal, a
parameter's value, a function the binder inlines) and computed when the
statement runs (a subquery, a `LET` in a block, a function that writes or
throws).

A vector of another dimension than the index's fails an ANN search. Brute
force ranks the records whose vector the distance applies to, so in a table of
mixed dimensions a record of another dimension than the query's is skipped
rather than failing the query: a 3-d query over 2-d records ranks none of them,
and over a table holding both it ranks only the 3-d ones.

A vector no distance ranks a record against is refused on both tiers, known
at planning or computed when the statement runs: an empty one, and one holding
a NaN or an infinity.

The brute-force vector is evaluated when the ranking first needs it, as the
ANN one is when its search opens: a `LIMIT 0` or a source with no row never
evaluates it, so a vector that writes leaves no write behind, and one that
throws or would be refused raises nothing; a vector known at planning is
refused at planning, whatever the statement then reads. A condition beneath
the ranking that rejects every row leaves the vector unevaluated too, whether
the scan runs its rows one at a time (a vector that writes) or in batches (one
that only reads and throws). Once a row is ranked, the vector is evaluated
once for every row. A vector that writes then throws fails the statement, and
its write is rolled back.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "NONE"

[[test.results]]
error = "Incorrect vector dimension (3). Expected a vector of 2 dimension."

[[test.results]]
error = "Incorrect vector dimension (3). Expected a vector of 2 dimension."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[mixed:three]"

[[test.results]]
value = "[mixed:three]"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = """
Invalid query: The KNN query vector must be a non-empty array of finite numbers: (
  Only
    Project[VALUE v]
      Source[vec:empty]
)"""

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = """
Invalid query: The KNN query vector must be a non-empty array of finite numbers: (
  Only
    Project[VALUE v]
      Source[vec:empty]
)"""

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: [<float> 'NaN', 0f]"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: [<float> 'NaN', 0f]"

[[test.results]]
error = """
Invalid query: The KNN query vector must be a non-empty array of finite numbers: (
  Only
    Project[VALUE v]
      Source[vec:nan]
)"""

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $q"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $q"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: [<float> 'NaN', 0f]"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: [<float> 'NaN', 0f]"

[[test.results]]
error = """
Invalid query: The KNN query vector must be a non-empty array of finite numbers: (
  Only
    Project[VALUE v]
      Source[vec:nan]
)"""

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $q"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $q"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[marker:1]"

[[test.results]]
value = "[bpts:5, bpts:4]"

[[test.results]]
value = "[bpts:5, bpts:4]"

[[test.results]]
value = "2"

[[test.results]]
error = "An error occurred: wboom"

[[test.results]]
error = "An error occurred: wboom"

[[test.results]]
value = "[marker:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[marker:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::this_arg([5f, 0f])"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::self_arg([5f, 0f])"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::parent_arg([5f, 0f])"

[[test.results]]
value = "[{ id: bpts:1, point: [1f, 0f] }]"

[[test.results]]
value = "[{ id: bpts:1, point: [1f, 0f] }]"

[[test.results]]
value = "[{ id: bpts:1, point: [1f, 0f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::reads_this()"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pa:1, pb:1]"

[[test.results]]
value = "1"

[[test.results]]
value = "[pa:2, pa:2]"

[[test.results]]
value = "2"

[[test.results]]
value = "[[pa:3, pb:3], [pa:4, pb:4]]"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $nan"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $nan"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $dnan"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $dnan"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: [$parent.x ?? 1f, 0f]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: [$parent.x ?? 1f, 0f]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: [$parent.x ?? 1f, 0f]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: [$parent.x ?? 1f, 0f]"

[[test.results]]
value = "[pts:4]"

[[test.results]]
value = "[bpts:4]"

[[test.results]]
value = "[bpts:4]"

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
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $n"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $dn"

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: $dn"

[[test.results]]
error = "Expected `number` but found `'a'` when coercing element at index 0 of `array<number>`"

[[test.results]]
error = "Expected `number` but found `'a'` when coercing element at index 0 of `array<number>`"

[[test.results]]
error = "Expected `number` but found `'a'` when coercing element at index 0 of `array<number>`"

[[test.results]]
error = "Expected `number` but found `'a'` when coercing element at index 0 of `array<number>`"

[[test.results]]
error = "Expected `number` but found `'a'` when coercing element at index 0 of `array<number>`"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[[pts:4]]"
*/

DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
FOR $i IN 0..6 {
	CREATE type::record('pts', $i) SET point = [<float> $i, 0f];
	CREATE type::record('bpts', $i) SET point = [<float> $i, 0f];
};
CREATE mixed:two SET point = [1f, 0f] RETURN NONE;
CREATE mixed:three SET point = [1f, 0f, 0f] RETURN NONE;
CREATE vec:empty SET v = [] RETURN NONE;
CREATE vec:nan SET v = [<float> 'NaN', 0f] RETURN NONE;
DEFINE TABLE vacant SCHEMALESS;
DEFINE TABLE marker SCHEMALESS;
DEFINE FUNCTION fn::v3() { RETURN [1f, 2f, 3f]; };
DEFINE FUNCTION fn::empty() { RETURN []; };
DEFINE FUNCTION fn::nan() { RETURN [<float> 'NaN', 0f]; };
DEFINE FUNCTION fn::w() { UPSERT marker:1 SET n = 1; RETURN [5f, 0f]; };
DEFINE FUNCTION fn::count() { UPSERT counter:1 SET n += 1; RETURN [5f, 0f]; };
DEFINE FUNCTION fn::boom() { THROW 'boom'; };
DEFINE FUNCTION fn::wboom() { CREATE marker:9 SET n = 1; THROW 'wboom'; };
-- 1. a vector of another dimension: an ANN search fails, brute force skips the records it does not apply to
SELECT VALUE id FROM pts WHERE point <|1,40|> [1f, 2f, 3f];
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::v3();
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [1f, 2f, 3f];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::v3();
SELECT VALUE id FROM mixed WHERE point <|2,EUCLIDEAN|> [1f, 0f, 0f];
SELECT VALUE id FROM mixed WHERE point <|2,EUCLIDEAN|> fn::v3();
-- 2. an empty vector: refused on both tiers, at planning where it is known then and when it is evaluated otherwise
SELECT VALUE id FROM pts WHERE point <|1,40|> [];
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::empty();
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM ONLY vec:empty);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::empty();
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM ONLY vec:empty);
-- 3. a NaN or an infinity: refused on both tiers, at planning where it is known then and when it is evaluated otherwise
SELECT VALUE id FROM pts WHERE point <|2,40|> [<float> 'NaN', 0f];
SELECT VALUE id FROM pts WHERE point <|2,40|> fn::nan();
SELECT VALUE id FROM pts WHERE point <|2,40|> (SELECT VALUE v FROM ONLY vec:nan);
{ LET $q = [math::inf, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|2,40|> $q; };
{ LET $q = [0f, -math::inf]; RETURN SELECT VALUE id FROM pts WHERE point <|2,40|> $q; };
SELECT VALUE id FROM bpts WHERE point <|2,EUCLIDEAN|> [<float> 'NaN', 0f];
SELECT VALUE id FROM bpts WHERE point <|2,EUCLIDEAN|> fn::nan();
SELECT VALUE id FROM bpts WHERE point <|2,EUCLIDEAN|> (SELECT VALUE v FROM ONLY vec:nan);
{ LET $q = [math::inf, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|2,EUCLIDEAN|> $q; };
{ LET $q = [0f, -math::inf]; RETURN SELECT VALUE id FROM bpts WHERE point <|2,EUCLIDEAN|> $q; };
-- 4. LIMIT 0 and a source with no row evaluate no vector, on either tier: no write, no error, no refusal of a vector evaluated when the statement runs
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::w() LIMIT 0;
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::w() LIMIT 0;
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> fn::w();
SELECT VALUE id FROM marker;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::boom() LIMIT 0;
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::boom() LIMIT 0;
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> fn::boom();
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM ONLY vec:empty) LIMIT 0;
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM ONLY vec:empty);
-- 5. a vector known at planning is refused there, whatever the statement then reads
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [] LIMIT 0;
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> [];
-- 6. once a row is ranked, the vector is evaluated, once for every row
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::w() LIMIT 1;
SELECT VALUE id FROM marker;
SELECT VALUE id FROM bpts WHERE point <|2,EUCLIDEAN|> fn::count();
SELECT VALUE id FROM bpts WHERE array::any([1], |$x| $x = 1) AND point <|2,EUCLIDEAN|> fn::count();
SELECT VALUE n FROM ONLY counter:1;
-- 7. a vector that writes then throws fails the statement, and its write is rolled back
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::wboom();
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::wboom();
SELECT VALUE id FROM marker;
-- 8. rows a condition beneath the ranking rejects are never ranked, so the vector is not evaluated
DEFINE FUNCTION fn::w2() { UPSERT marker:2 SET n = 1; RETURN [5f, 0f]; };
SELECT VALUE id FROM bpts WHERE string::len(<string> id) > 100 AND point <|1,EUCLIDEAN|> fn::w2();
SELECT VALUE id FROM bpts WHERE string::len(<string> id) > 100 AND point <|1,EUCLIDEAN|> fn::boom();
SELECT VALUE id FROM bpts WHERE array::any([1], |$x| $x = 2) AND point <|1,EUCLIDEAN|> fn::w2();
SELECT VALUE id FROM marker;
-- 9. a function body called with a row reads that row as its `$this`, `$self` and `$parent`, over an argument of the name: in a vector it is the record being scanned, an error when the vector is evaluated
DEFINE FUNCTION fn::this_arg($this: any) { UPSERT marker:3 SET n = 1; RETURN $this; };
DEFINE FUNCTION fn::self_arg($self: any) { UPSERT marker:3 SET n = 1; RETURN $self; };
DEFINE FUNCTION fn::parent_arg($parent: any) { UPSERT marker:3 SET n = 1; RETURN $parent; };
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::this_arg([5f, 0f]);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::self_arg([5f, 0f]);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::parent_arg([5f, 0f]);
SELECT VALUE fn::this_arg([5f, 0f]) FROM bpts:1;
SELECT VALUE fn::self_arg([5f, 0f]) FROM bpts:1;
SELECT VALUE fn::parent_arg([5f, 0f]) FROM bpts:1;
DEFINE FUNCTION fn::reads_this() { UPSERT marker:3 SET n = 1; RETURN $this.point; };
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::reads_this();
-- 10. one KNN operator over several sources searches them all with one evaluation of the vector (a source after a search keeps the records it found), a correlated one once per outer row
DEFINE INDEX ha ON pa FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
DEFINE INDEX hb ON pb FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
FOR $i IN 0..6 { CREATE type::record('pa', $i) SET point = [<float> $i, 0f]; CREATE type::record('pb', $i) SET point = [<float> $i, 0f]; };
DEFINE FUNCTION fn::step() { UPSERT counter:2 SET n += 1; RETURN [<float> counter:2.n, 0f]; };
SELECT VALUE id FROM pa, pb WHERE point <|1,40|> fn::step();
SELECT VALUE n FROM ONLY counter:2;
SELECT VALUE id FROM pa, pa:2 WHERE point <|1,40|> fn::step();
SELECT VALUE n FROM ONLY counter:2;
SELECT VALUE (SELECT VALUE id FROM pa, pb WHERE point <|1,40|> fn::step()) FROM [1, 2];
SELECT VALUE n FROM ONLY counter:2;
-- 11. a vector a parameter holds when the statement is planned, a statement's or a defined one, is refused when empty or non-finite on both tiers, and searched with otherwise
LET $nan = [<float> 'NaN', 0f];
LET $empty = [];
DEFINE PARAM $dnan VALUE [0f, <float> 'inf'];
DEFINE PARAM $dfive VALUE [5f, 0f];
SELECT VALUE id FROM pts WHERE point <|1,40|> $nan;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $nan;
SELECT VALUE id FROM pts WHERE point <|1,40|> $empty;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $empty;
SELECT VALUE id FROM pts WHERE point <|1,40|> $dnan;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $dnan;
SELECT VALUE id FROM pts WHERE point <|1,40|> $dfive;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $dfive;
-- 12. a `$parent` nothing around the statement binds falls back to the document, per row the record being scanned: refused at planning on both tiers, even over a table with no row and in a block; a statement nested in another reads that statement's row, and a SELECT of the vector binds its own
SELECT VALUE id FROM pts WHERE point <|1,40|> [$parent.x ?? 1f, 0f];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [$parent.x ?? 1f, 0f];
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> [$parent.x ?? 1f, 0f];
{ RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> [$parent.x ?? 1f, 0f]; };
SELECT VALUE (SELECT VALUE id FROM pts WHERE point <|1,40|> [$parent.x ?? 1f, 0f]) FROM ONLY { x: 4f };
SELECT VALUE (SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [$parent.x ?? 1f, 0f]) FROM ONLY { x: 4f };
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE [[4f, 0f]][WHERE $parent.k = 1][0] FROM ONLY { k: 1 });
-- 13. a parameter's value known at planning is classified by that value, as the vector written in its place is: one that is empty, non-finite or not an array of numbers is refused at planning, even over a table with no row or under a condition no row meets; a name a block `LET` or a closure binds has no value at planning
LET $e = [];
LET $n = [0f, <float> 'NaN'];
LET $s = ['a', 0f];
DEFINE PARAM $de VALUE [];
DEFINE PARAM $dn VALUE [<float> '-inf', 0f];
DEFINE PARAM $ds VALUE ['a', 0f];
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> $e;
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> $de;
SELECT VALUE id FROM bpts WHERE string::len(<string> id) > 100 AND point <|1,EUCLIDEAN|> $de;
SELECT VALUE id FROM pts WHERE string::len(<string> id) > 100 AND point <|1,40|> $de;
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> $n;
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> $dn;
SELECT VALUE id FROM pts WHERE string::len(<string> id) > 100 AND point <|1,40|> $dn;
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> ['a', 0f];
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> $s;
SELECT VALUE id FROM vacant WHERE point <|1,EUCLIDEAN|> $ds;
SELECT VALUE id FROM bpts WHERE string::len(<string> id) > 100 AND point <|1,EUCLIDEAN|> $ds;
SELECT VALUE id FROM pts WHERE string::len(<string> id) > 100 AND point <|1,40|> $ds;
{ LET $ds = [5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $ds; };
RETURN [[4f, 0f]].map(|$de| SELECT VALUE id FROM pts WHERE point <|1,40|> $de);
