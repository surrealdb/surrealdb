/**
[env]
namespace = true
database = true

[env.capabilities]
allow-functions = true
allow-eval-query = true
allow-arbitrary-query = true

[test]
reason = """
The contract for a KNN query vector, for an HNSW index (`pts`, `<|k,EF|>`) and
for brute force (`bpts`, `<|k,DIST|>`).

A right-hand side that does not read the record being scanned is the query
vector: a cast, a negation, a binary expression, a path rooted at a parameter,
a call, an `IF`, an object, an uncorrelated subquery, a closure passed as a
value. It is evaluated once, before the search reads anything, and its value
must coerce to `array<number>`; a value that does not, or an evaluation that
fails, fails the query. `$parent` bound by an enclosing statement is that
statement's row, so a correlated subquery searches with each outer row's
vector.

A right-hand side that reads the record being scanned fails planning: a field,
an array holding one, `$this`, a projection builtin (`type::field`), a
subquery correlated through `$parent`, a `$parent` in a path's `[WHERE …]`,
and a destructure of the record. Inside an aliased destructure, the bodies
read the destructured value. Inside a SELECT of the vector, a `[WHERE …]`, a
lookup, a destructure body or a block in its projection or condition binds
`$parent` to that SELECT's row, while one at the SELECT's own level, in its
source, or in a filter of its source is the record being scanned.

A read the plan cannot see fails when the vector is evaluated, with the same
error: a function body or a closure reading a field or `$this`, a script
receiving the record as its document, a `[WHERE …]` or lookup hidden in a
closure binding `$parent` to the record, a projection builtin hidden in a
closure, a closure invoked inside a SELECT of the vector that reads
`$parent`, and a statement in a closure body taking the record as `$this` or
`$parent`. A function called in a `[WHERE …]` over a parameter reads the
element, not the record, and is evaluated once.
A function, closure or middleware that writes but reads only its arguments is
evaluated once. A closure parameter named `$this`, `$self` or `$parent` is its
argument, as it is per row.

A WHERE clause supports a single KNN operator. One inside a closure literal is
not one of the condition: it is evaluated as an expression when the closure is
invoked, which no search answers. On the left of the operator, a projection
naming a field (`type::field('point')`) is that field.

`eval::surql` runs its statement isolated from the call site, with only its
explicit bindings: it reaches no record being scanned, so a vector it computes
is evaluated once, and a field or `$parent` it reads is `NONE`.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Cannot negate the value 'array'"

[[test.results]]
error = "Cannot negate the value 'array'"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Expected `array<number>` but found `{  }`"

[[test.results]]
error = "Expected `array<number>` but found `{  }`"

[[test.results]]
error = "Could not cast into `array` using input `'abc'`"

[[test.results]]
error = "Could not cast into `array` using input `'abc'`"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: point"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: point"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: [point, 0f]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: [point, 0f]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $this.point"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $this.point"

[[test.results]]
value = "[{ id: q:a, v: [5f, 0f] }]"

[[test.results]]
value = "[{ id: q:b, v: [1f, 0f] }]"

[[test.results]]
value = "[{ id: q:a, n: [pts:5] }, { id: q:b, n: [pts:1] }]"

[[test.results]]
value = "[{ id: q:a, n: [bpts:5] }, { id: q:b, n: [bpts:1] }]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $parent.v"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $parent.v"

[[test.results]]
value = "[{ id: qv:one, n: 5f, v: [5f, 0f] }]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Filter[n = $parent.point[0]]
      Source[qv]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Filter[n = $parent.point[0]]
      Source[qv]
)[0]"""

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: [{ enabled: true, v: [5f, 0f] }][WHERE $parent.enabled][0].v"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: [{ enabled: true, v: [5f, 0f] }][WHERE $parent.enabled][0].v"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: array::map([1, 2], |$x: any| (IF enabled THEN $x ELSE 0 END))"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: array::map([1, 2], |$x: any| (IF enabled THEN $x ELSE 0 END))"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Expected `array<number>` but found `{ a: 1 }`"

[[test.results]]
error = "Expected `array<number>` but found `{ a: 1 }`"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: type::field('point')"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: type::field('point')"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::reads()"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::reads()"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::reads_this()"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::reads_this()"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: function() { return this.point; }"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: function() { return this.point; }"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::w()"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: fn::w()"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.vector(1)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.vector(1)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.vector(1)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.vector(1)"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.f(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.f(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.f(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.f(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: array::map([0], $f)[0]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: array::map([0], $f)[0]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:4]"

[[test.results]]
value = "[bpts:4]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.f(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.f(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.f(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: $o.f(0)"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: ($f)(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: ($f)(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: ($f)(0)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: ($f)(0)"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: api::timeout({  }, |$r: any| [x ?? 5f, 0f], 1s)"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: api::timeout({  }, |$r: any| [x ?? 5f, 0f], 1s)"

[[test.results]]
value = "[pts:4]"

[[test.results]]
value = "[bpts:4]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:4]"

[[test.results]]
value = "[bpts:4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:0, pts:1, pts:2, pts:3, pts:4, pts:5]"

[[test.results]]
value = "[bpts:0, bpts:1, bpts:2, bpts:3, bpts:4, bpts:5]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: A WHERE clause supports a single KNN operator (<|k, …|>); found 2."

[[test.results]]
error = "Invalid query: A WHERE clause supports a single KNN operator (<|k, …|>); found 2."

[[test.results]]
error = "Invalid query: A WHERE clause supports a single KNN operator (<|k, …|>); found 2."

[[test.results]]
error = "Invalid query: A WHERE clause supports a single KNN operator (<|k, …|>); found 2."

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:4]"

[[test.results]]
value = "[bpts:4]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: point.{ v: [5f, 0f][WHERE $parent.enabled] }.v"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: point.{ v: [5f, 0f][WHERE $parent.enabled] }.v"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: { p: point }.{ v: [5f, 0f][WHERE $parent.p] }.v"

[[test.results]]
error = "Invalid query: The KNN query vector cannot depend on the record being scanned: { p: point }.{ v: [5f, 0f][WHERE $parent.p] }.v"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:4]"

[[test.results]]
value = "[bpts:4]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE $parent.v]
    Source[qv]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE $parent.v]
    Source[qv]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Source[[{ v: [5f, 0f] }][WHERE $parent.enabled]]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Source[[{ v: [5f, 0f] }][WHERE $parent.enabled]]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Only
    Project[VALUE $parent.vs[WHERE enabled][0]]
      Source[{ k: 1 }]
)"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Only
    Project[VALUE $parent.vs[WHERE enabled][0]]
      Source[{ k: 1 }]
)"""

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Only
    Project[VALUE [1].map(|$y: any| ($parent.v))[0]]
      Source[{ v: [5f, 0f] }]
)"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Only
    Project[VALUE [1].map(|$y: any| ($parent.v))[0]]
      Source[{ v: [5f, 0f] }]
)"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Source[[1].map(|$y: any| { v: $parent.v })]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Source[[1].map(|$y: any| { v: $parent.v })]
)[0]"""

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:4]"

[[test.results]]
value = "[bpts:4]"

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Source[[{ enabled: true, v: [5f, 0f] }][WHERE $parent.enabled]]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Source[[{ enabled: true, v: [5f, 0f] }][WHERE $parent.enabled]]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Only
    Project[VALUE v]
      Source[{ v: [5f, 0f] }.{ v: $parent.v }]
)"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Only
    Project[VALUE v]
      Source[{ v: [5f, 0f] }.{ v: $parent.v }]
)"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Only
    Project[VALUE v]
      Source[$parent.{ v }]
)"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Only
    Project[VALUE v]
      Source[$this.{ v }]
)"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Source[(
      Sequence
        Return
          Eval[[{ v: $parent.v }]]
    )]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    Source[(
      Sequence
        Return
          Eval[[{ v: $parent.v }]]
    )]
)[0]"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (|$this: any| ((
  Project[VALUE v]
    Source[$this]
)[0]))([{ v: [5f, 0f] }])"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (|$parent: any| ((
  Project[VALUE v]
    Source[$parent]
)[0]))([{ v: [5f, 0f] }])"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (|$self: any| (
  Sequence
    Return
      Eval[$self[0].v]
))([{ v: [5f, 0f] }])"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (|$parent: any| (
  Sequence
    Return
      Eval[$parent[0].v]
))([{ v: [5f, 0f] }])"""

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"
*/

DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
FOR $i IN 0..6 {
	CREATE type::record('pts', $i) SET point = [<float> $i, 0f];
	CREATE type::record('bpts', $i) SET point = [<float> $i, 0f];
};
-- 1. a cast
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> <array<float>> $q; };
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> <array<float>> $q; };
-- 2. a negation, which arrays do not support
{ LET $q = [-5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> -$q; };
{ LET $q = [-5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> -$q; };
-- 3. a binary expression with an array operand
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> ($q ?? [0f, 0f]); };
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> ($q ?? [0f, 0f]); };
-- 4. a path to a value that is not a vector
{ LET $o = { v: {} }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.v; };
{ LET $o = { v: {} }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.v; };
-- 5. a call that fails
{ LET $s = 'abc'; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> type::array($s); };
{ LET $s = 'abc'; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> type::array($s); };
-- 6. a vector that reads the record being scanned: a planning error
SELECT VALUE id FROM pts WHERE point <|1,40|> point;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> point;
SELECT VALUE id FROM pts WHERE point <|1,40|> [point, 0f];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [point, 0f];
SELECT VALUE id FROM pts WHERE point <|1,40|> $this.point;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $this.point;
-- 7. a correlated `$parent`: each outer row searches with its own vector
CREATE q:a SET v = [5f, 0f];
CREATE q:b SET v = [1f, 0f];
SELECT id, (SELECT VALUE id FROM pts WHERE point <|1,40|> $parent.v) AS n FROM q ORDER BY id;
SELECT id, (SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $parent.v) AS n FROM q ORDER BY id;
-- 8. `$parent` with no enclosing statement is the record being scanned: a planning error
SELECT VALUE id FROM pts WHERE point <|1,40|> $parent.v;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $parent.v;
-- 9. an uncorrelated subquery, as an array element and as a path root
CREATE qv:one SET n = 5f, v = [5f, 0f];
SELECT VALUE id FROM pts WHERE point <|1,40|> [(SELECT VALUE n FROM ONLY qv:one), 0f];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [(SELECT VALUE n FROM ONLY qv:one), 0f];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM qv:one)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM qv:one)[0];
-- 10. a correlated subquery: a planning error
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM qv WHERE n = $parent.point[0])[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM qv WHERE n = $parent.point[0])[0];
-- 11. `$parent` in a path's `[WHERE …]` is the record being scanned: a planning error
{ LET $vectors = [{ enabled: true, v: [5f, 0f] }]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $vectors[WHERE $parent.enabled][0].v; };
{ LET $vectors = [{ enabled: true, v: [5f, 0f] }]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $vectors[WHERE $parent.enabled][0].v; };
-- 12. a field in a path's `[WHERE …]` is the element: evaluated once
{ LET $vectors = [{ enabled: false, v: [1f, 0f] }, { enabled: true, v: [5f, 0f] }]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $vectors[WHERE enabled][0].v; };
{ LET $vectors = [{ enabled: false, v: [1f, 0f] }, { enabled: true, v: [5f, 0f] }]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $vectors[WHERE enabled][0].v; };
-- 13. a closure whose body reads the record: an error when the vector is evaluated
SELECT VALUE id FROM pts WHERE point <|1,40|> array::map([1, 2], |$x| IF enabled THEN $x ELSE 0 END);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> array::map([1, 2], |$x| IF enabled THEN $x ELSE 0 END);
-- 14. a closure whose body reads only its argument: evaluated once
SELECT VALUE id FROM pts WHERE point <|1,40|> array::map([5, 0], |$x| <float> $x);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> array::map([5, 0], |$x| <float> $x);
-- 15. an `IF` in an array element
{ LET $high = true; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> [IF $high THEN 5f ELSE 1f END, 0f]; };
{ LET $high = true; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [IF $high THEN 5f ELSE 1f END, 0f]; };
-- 16. an object, which is not a vector
SELECT VALUE id FROM pts WHERE point <|1,40|> { a: 1 };
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> { a: 1 };
-- 17. a path rooted at a call
{ LET $v = [5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> object::from_entries([['v', $v]]).v; };
{ LET $v = [5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> object::from_entries([['v', $v]]).v; };
-- 18. a projection builtin reads the record's fields by name: a planning error
{ LET $f = 'point'; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> type::field($f); };
{ LET $f = 'point'; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> type::field($f); };
-- 19. a function body that reads its caller's row: an error when the vector is evaluated
DEFINE FUNCTION fn::reads() { RETURN [point, 0f]; };
DEFINE FUNCTION fn::reads_this() { RETURN $this.point; };
DEFINE FUNCTION fn::v($x: number) { RETURN [<float> $x, 0f]; };
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::reads();
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::reads();
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::reads_this();
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::reads_this();
-- 20. a script receives the record as its document: an error when the vector is evaluated
SELECT VALUE id FROM pts WHERE point <|1,40|> function() { return this.point; };
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> function() { return this.point; };
-- 21. a function that reads only its arguments: evaluated once
{ LET $x = 5; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> fn::v($x); };
{ LET $x = 5; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::v($x); };
-- 22. a function body that writes: an error where it reads the record, evaluated once where it does not
DEFINE FUNCTION fn::w() { UPSERT marker:1 SET n = 1; RETURN [point ?? 0f, 0f]; };
DEFINE FUNCTION fn::w_pure() { UPSERT marker:2 SET n = 1; RETURN [5f, 0f]; };
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::w();
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::w();
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::w_pure();
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::w_pure();
-- 23. a closure held by a bound object and called as a method runs with the record in scope, not its receiver
{ LET $o = { point: [4f, 0f], vector: |$x| point }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.vector(1); };
{ LET $o = { point: [4f, 0f], vector: |$x| point }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.vector(1); };
{ LET $o = { point: [4f, 0f], vector: |$x| $this.point }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.vector(1); };
{ LET $o = { point: [4f, 0f], vector: |$x| $this.point }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.vector(1); };
{ LET $o = { vector: |$x| [<float> $x, 0f] }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.vector(5); };
{ LET $o = { vector: |$x| [<float> $x, 0f] }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.vector(5); };
-- 24. a `[WHERE …]` or a lookup hidden in a closure binds `$parent` to the record: an error when the vector is evaluated
CREATE person:a, thing:x RETURN NONE;
RELATE person:a->likes->thing:x SET w = 4, v = [4f, 0f] RETURN NONE;
{ LET $o = { f: |$x| [[5f, 0f]][WHERE $parent.enabled][0] ?? [1f, 0f] }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.f(0); };
{ LET $o = { f: |$x| [[5f, 0f]][WHERE $parent.enabled][0] ?? [1f, 0f] }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.f(0); };
{ LET $o = { f: |$x| person:a->likes[WHERE w = $parent.w][0].v ?? [1f, 0f] }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.f(0); };
{ LET $o = { f: |$x| person:a->likes[WHERE w = $parent.w][0].v ?? [1f, 0f] }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.f(0); };
{ LET $f = |$x| [[5f, 0f]][WHERE $parent.enabled][0] ?? [1f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> array::map([0], $f)[0]; };
{ LET $f = |$x| [[5f, 0f]][WHERE $parent.enabled][0] ?? [1f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> array::map([0], $f)[0]; };
-- 25. a hidden `[WHERE …]` or lookup that does not read `$parent`: evaluated once
{ LET $o = { f: |$x| [[5f, 0f], [1f, 0f]][WHERE $this[0] > 2][0] }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.f(0); };
{ LET $o = { f: |$x| [[5f, 0f], [1f, 0f]][WHERE $this[0] > 2][0] }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.f(0); };
{ LET $o = { f: |$x| person:a->likes[WHERE w = 4][0].v }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.f(0); };
{ LET $o = { f: |$x| person:a->likes[WHERE w = 4][0].v }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.f(0); };
-- 26. inside a closure body evaluated against an element, a subquery binding the record as its `$parent` and an index expression reading the record: an error when the vector is evaluated
{ LET $o = { f: |$x| [{ enabled: false, v: [5f, 0f] }][WHERE !(SELECT VALUE $parent.enabled FROM ONLY 1)][0].v ?? [1f, 0f] }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.f(0); };
{ LET $o = { f: |$x| [{ enabled: false, v: [5f, 0f] }][WHERE !(SELECT VALUE $parent.enabled FROM ONLY 1)][0].v ?? [1f, 0f] }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.f(0); };
{ LET $o = { f: |$x| [[5f, 0f]][point] ?? [1f, 0f] }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.f(0); };
{ LET $o = { f: |$x| [[5f, 0f]][point] ?? [1f, 0f] }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.f(0); };
-- 27. the same shapes when they do not reach the record: evaluated once
{ LET $o = { f: |$x| [{ v: [5f, 0f] }][WHERE (SELECT VALUE true FROM ONLY 1)][0].v }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.f(0); };
{ LET $o = { f: |$x| [{ v: [5f, 0f] }][WHERE (SELECT VALUE true FROM ONLY 1)][0].v }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.f(0); };
{ LET $o = { f: |$x| [[1f, 0f], [5f, 0f]][$x] }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.f(1); };
{ LET $o = { f: |$x| [[1f, 0f], [5f, 0f]][$x] }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.f(1); };
-- 28. a projection builtin hidden in a closure: an error when the vector is evaluated
{ LET $f = |$x| [type::field("point")[0] ?? 5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $f(0); };
{ LET $f = |$x| [type::field("point")[0] ?? 5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $f(0); };
{ LET $f = |$x| type::fields(["point"])[0] ?? [5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $f(0); };
{ LET $f = |$x| type::fields(["point"])[0] ?? [5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $f(0); };
-- 29. a builtin hidden in a closure that does not read the record: evaluated once
{ LET $f = |$x| [<float> string::len("abcde"), 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $f(0); };
{ LET $f = |$x| [<float> string::len("abcde"), 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $f(0); };
-- 30. a closure that is only passed along is a value, whatever its body reads: evaluated once
DEFINE FUNCTION fn::constant($f: any) { RETURN [5f, 0f]; };
SELECT VALUE id FROM pts WHERE point <|1,40|> fn::constant(|$x| point);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> fn::constant(|$x| point);
-- 31. a projection on the left names the field searched, for an index and for brute force, on a static and a runtime-resolved source
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE type::field("point") <|1,40|> $q; };
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE type::field("point") <|1,EUCLIDEAN|> $q; };
{ LET $q = [5f, 0f]; LET $tb = "bpts"; RETURN SELECT VALUE id FROM type::table($tb) WHERE type::field("point") <|1,EUCLIDEAN|> $q; };
-- 32. API middleware runs its closure with the record in scope: an error where the closure reads it, evaluated once where it does not
SELECT VALUE id FROM pts WHERE point <|1,40|> api::timeout({}, |$r| [x ?? 5f, 0f], 1s);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> api::timeout({}, |$r| [x ?? 5f, 0f], 1s);
SELECT VALUE id FROM pts WHERE point <|1,40|> api::timeout({}, |$r| [4f, 0f], 1s);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> api::timeout({}, |$r| [4f, 0f], 1s);
-- 33. inside a SELECT of the vector, a `[WHERE …]` or a lookup over one of its own rows binds `$parent` to that row: evaluated once
{ LET $f = |$x| [[5f, 0f]][WHERE $parent.enabled][0]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE $f(0) FROM ONLY { enabled: true }); };
{ LET $f = |$x| [[5f, 0f]][WHERE $parent.enabled][0]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE $f(0) FROM ONLY { enabled: true }); };
{ LET $f = |$x| person:a->likes[WHERE w = $parent.w][0].v; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE $f(0) FROM ONLY { w: 4 }); };
{ LET $f = |$x| person:a->likes[WHERE w = $parent.w][0].v; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE $f(0) FROM ONLY { w: 4 }); };
-- 34. a KNN operator inside a closure literal is no KNN operator of the condition: a closure only passed along lets every row through
DEFINE FUNCTION fn::accept($f: any) { RETURN true; };
SELECT VALUE id FROM pts WHERE fn::accept(|$x| point <|1,40|> point) ORDER BY id;
SELECT VALUE id FROM bpts WHERE fn::accept(|$x| point <|1,EUCLIDEAN|> point) ORDER BY id;
-- 35. one inside an invoked closure is evaluated as an expression, which no search answers: no row
SELECT VALUE id FROM bpts WHERE array::any([1], |$x| point <|1,EUCLIDEAN|> [1f, 0f]) ORDER BY id;
-- 36. a WHERE clause supports a single KNN operator: two are refused rather than one served and the other dropped
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [1f, 0f] AND point <|1,EUCLIDEAN|> [4f, 0f];
SELECT VALUE id FROM pts WHERE point <|1,40|> [1f, 0f] AND point <|1,40|> [4f, 0f];
SELECT VALUE id FROM pts WHERE point <|1,40|> [1f, 0f] AND point <|1,EUCLIDEAN|> [4f, 0f];
{ LET $tb = "bpts"; RETURN SELECT VALUE id FROM type::table($tb) WHERE point <|1,EUCLIDEAN|> [1f, 0f] AND point <|1,EUCLIDEAN|> [4f, 0f]; };
-- 37. a second KNN operator inside a closure literal is not one of the condition: the single real one is served
SELECT VALUE id FROM pts WHERE point <|1,40|> [5f, 0f] AND fn::accept(|$x| point <|1,40|> [1f, 0f]);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [5f, 0f] AND fn::accept(|$x| point <|1,EUCLIDEAN|> [1f, 0f]);
-- 38. an aliased destructure binds its bodies' `$parent` and document to the destructured value: evaluated once
{ LET $o = { enabled: true }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.{ v: [5f, 0f][WHERE $parent.enabled] }.v; };
{ LET $o = { enabled: true }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.{ v: [5f, 0f][WHERE $parent.enabled] }.v; };
{ LET $o = { w: 4 }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.{ v: person:a->likes[WHERE w = $parent.w][0].v }.v; };
{ LET $o = { w: 4 }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.{ v: person:a->likes[WHERE w = $parent.w][0].v }.v; };
{ LET $o = { n: 5f }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.{ v: (SELECT VALUE v FROM qv WHERE n = $parent.n)[0] }.v; };
{ LET $o = { n: 5f }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.{ v: (SELECT VALUE v FROM qv WHERE n = $parent.n)[0] }.v; };
{ LET $o = { i: 1 }; LET $vs = [[1f, 0f], [5f, 0f]]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.{ v: $vs[i] }.v; };
{ LET $o = { i: 1 }; LET $vs = [[1f, 0f], [5f, 0f]]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.{ v: $vs[i] }.v; };
-- 39. a destructure of the record, or of a value built from it: a planning error
SELECT VALUE id FROM pts WHERE point <|1,40|> point.{ v: [5f, 0f][WHERE $parent.enabled] }.v;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> point.{ v: [5f, 0f][WHERE $parent.enabled] }.v;
SELECT VALUE id FROM pts WHERE point <|1,40|> { p: point }.{ v: [5f, 0f][WHERE $parent.p] }.v;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> { p: point }.{ v: [5f, 0f][WHERE $parent.p] }.v;
-- 40. a closure whose body writes is evaluated once: the record its write returns is not the record being scanned
LET $writes = |$x| { LET $v = CREATE ONLY made SET at = $x RETURN VALUE [point ?? 5f, 0f]; RETURN $v; };
SELECT VALUE id FROM pts WHERE point <|1,40|> $writes(1);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $writes(2);
SELECT VALUE at FROM made ORDER BY at;
-- 41. a function called in a `[WHERE …]` over a parameter reads the element, whether its body writes or not: evaluated once
DEFINE FUNCTION fn::reads_elem() { RETURN enabled; };
DEFINE FUNCTION fn::writes_elem() { UPSERT marker:3 SET n = 1; RETURN enabled; };
{ LET $vs = [{ enabled: false, v: [1f, 0f] }, { enabled: true, v: [5f, 0f] }]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $vs[WHERE fn::reads_elem()][0].v; };
{ LET $vs = [{ enabled: false, v: [1f, 0f] }, { enabled: true, v: [5f, 0f] }]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $vs[WHERE fn::writes_elem()][0].v; };
-- 42. inside a SELECT of the vector, a `[WHERE …]`, a lookup, a destructure body or a block evaluated per row binds `$parent` to that row: evaluated once
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE [[5f, 0f]][WHERE $parent.enabled][0] FROM ONLY { enabled: true });
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE [[5f, 0f]][WHERE $parent.enabled][0] FROM ONLY { enabled: true });
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE person:a->likes[WHERE w = $parent.w][0].v FROM ONLY { w: 4 });
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE person:a->likes[WHERE w = $parent.w][0].v FROM ONLY { w: 4 });
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE [[5f, 0f]][WHERE $parent.n = 5f][0] FROM qv LIMIT 1)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE [[5f, 0f]][WHERE $parent.n = 5f][0] FROM qv LIMIT 1)[0];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM qv WHERE [1][WHERE $parent.n = 5f])[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM qv WHERE [1][WHERE $parent.n = 5f])[0];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE { o: 1 }.{ v: [[5f, 0f]][WHERE $parent.o = 1][0] }.v FROM ONLY { e: 1 });
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE { o: 1 }.{ v: [[5f, 0f]][WHERE $parent.o = 1][0] }.v FROM ONLY { e: 1 });
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE { RETURN $parent.v } FROM qv)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE { RETURN $parent.v } FROM qv)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE (SELECT VALUE [[5f, 0f]][WHERE $parent.enabled][0] FROM ONLY { enabled: true }) FROM ONLY { k: 1 });
-- 43. a `$parent` at the SELECT's own level, in its source or as the base of a filter is the record being scanned: a planning error
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE $parent.v FROM qv)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE $parent.v FROM qv)[0];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM [{ v: [5f, 0f] }][WHERE $parent.enabled])[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM [{ v: [5f, 0f] }][WHERE $parent.enabled])[0];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE $parent.vs[WHERE enabled][0] FROM ONLY { k: 1 });
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE $parent.vs[WHERE enabled][0] FROM ONLY { k: 1 });
-- 44. inside a SELECT of the vector, a closure literal only passed along is a value: evaluated once
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE fn::constant(|$x| $parent.point) FROM ONLY { enabled: true });
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE fn::constant(|$x| $parent.point) FROM ONLY { enabled: true });
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE fn::constant($this) FROM fn::constant(|$x| $parent.point))[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE fn::constant($this) FROM fn::constant(|$x| $parent.point))[0];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM qv WHERE fn::constant(|$x| $parent.point) = [5f, 0f])[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM qv WHERE fn::constant(|$x| $parent.point) = [5f, 0f])[0];
-- 45. a closure invoked inside that SELECT whose body reads `$parent`, the record being scanned: an error when the vector is evaluated
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE [1].map(|$y| $parent.v)[0] FROM ONLY { v: [5f, 0f] });
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE [1].map(|$y| $parent.v)[0] FROM ONLY { v: [5f, 0f] });
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM [1].map(|$y| { v: $parent.v }))[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM [1].map(|$y| { v: $parent.v }))[0];
-- 46. one invoked whose body reads its argument or the SELECT's own row: evaluated once
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE [[5f, 0f]].map(|$y| $y)[0] FROM ONLY { k: 1 });
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE [[5f, 0f]].map(|$y| $y)[0] FROM ONLY { k: 1 });
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE [1].map(|$y| $this.v)[0] FROM ONLY { v: [5f, 0f] });
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE [1].map(|$y| $this.v)[0] FROM ONLY { v: [5f, 0f] });
-- 47. a destructure of a parameter in a SELECT's source binds its bodies' `[WHERE …]` and lookups to the destructured value: evaluated once
{ LET $o = { enabled: true }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM ONLY $o.{ v: [[5f, 0f]][WHERE $parent.enabled][0] }); };
{ LET $o = { enabled: true }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM ONLY $o.{ v: [[5f, 0f]][WHERE $parent.enabled][0] }); };
{ LET $o = { w: 4 }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM ONLY $o.{ v: person:a->likes[WHERE w = $parent.w][0].v }); };
{ LET $o = { w: 4 }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM ONLY $o.{ v: person:a->likes[WHERE w = $parent.w][0].v }); };
-- 48. a `[WHERE …]` in the source, a bare `$parent` in a destructure body, a source destructuring `$parent` or `$this`, and a block reading `$parent`: the record being scanned, a planning error
{ LET $vs = [{ enabled: true, v: [5f, 0f] }]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM $vs[WHERE $parent.enabled])[0]; };
{ LET $vs = [{ enabled: true, v: [5f, 0f] }]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM $vs[WHERE $parent.enabled])[0]; };
{ LET $o = { v: [5f, 0f] }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM ONLY $o.{ v: $parent.v }); };
{ LET $o = { v: [5f, 0f] }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM ONLY $o.{ v: $parent.v }); };
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM ONLY $parent.{ v });
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM ONLY $this.{ v });
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM { RETURN [{ v: $parent.v }] })[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM { RETURN [{ v: $parent.v }] })[0];
-- 49. a closure parameter named `$parent`, `$this` or `$self` is its argument, as it is per row, and a `$self` survives a scalar subquery: evaluated once
DEFINE FUNCTION fn::self_source() { RETURN (SELECT VALUE v FROM $self)[0]; };
SELECT VALUE id FROM pts WHERE point <|1,40|> [<float> (|$parent| $parent)(5), 0f];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> [<float> (|$this| $this)(5), 0f];
SELECT VALUE id FROM pts WHERE point <|1,40|> array::map([5, 0], |$self| <float> $self);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (|$self| (SELECT VALUE v FROM $self)[0])([{ v: [5f, 0f] }]);
SELECT VALUE id FROM pts WHERE point <|1,40|> (|$self| fn::self_source())([{ v: [5f, 0f] }]);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (|$this| (SELECT VALUE $this.v FROM qv)[0])([{ v: [5f, 0f] }]);
-- 50. a scalar subquery or a block in a closure body takes the row in scope, the record being scanned, as its `$this` and `$parent`: an error when the vector is evaluated
SELECT VALUE id FROM pts WHERE point <|1,40|> (|$this| (SELECT VALUE v FROM $this)[0])([{ v: [5f, 0f] }]);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (|$parent| (SELECT VALUE v FROM $parent)[0])([{ v: [5f, 0f] }]);
SELECT VALUE id FROM pts WHERE point <|1,40|> (|$self| { RETURN $self[0].v })([{ v: [5f, 0f] }]);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (|$parent| { RETURN $parent[0].v })([{ v: [5f, 0f] }]);
-- 51. `eval::surql` is isolated from the call site: a field or `$parent` it reads is NONE, not the record being scanned, and the vector is evaluated once
SELECT VALUE id FROM pts WHERE point <|1,40|> eval::surql('RETURN [5f, 0f]');
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> eval::surql('RETURN [point ?? 5f, 0f]');
SELECT VALUE id FROM pts WHERE point <|1,40|> eval::surql('RETURN [$parent.point[0] ?? 5f, 0f]');
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> eval::surql('RETURN [$this.point[0] ?? 5f, 0f]');
