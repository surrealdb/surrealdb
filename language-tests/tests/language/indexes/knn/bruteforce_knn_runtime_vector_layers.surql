/**
[env]
namespace = true
database = true

[test]
reason = """
Brute-force KNN whose query vector is computed when the statement runs, over a
table with a computed field. The conditions written beside the search, before
or after it, judge the rows first and the search ranks what they leave; the
computed field is readable in those conditions and in the projection. The
vector is evaluated the first time a row is ranked against it, so over a table
with no rows it is never evaluated: a valid one and one that would not coerce
to `array<number>` both yield nothing.
"""

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
value = "[{ id: pts:1, point: [1f, 1f], tag: 'a', upper: 'A' }]"

[[test.results]]
value = "[{ id: pts:2, point: [2f, 2f], tag: 'b', upper: 'B' }]"

[[test.results]]
value = "[{ id: pts:3, point: [3f, 3f], tag: 'a', upper: 'A' }]"

[[test.results]]
value = "[{ id: pts:4, point: [10f, 10f], tag: 'b', upper: 'B' }]"

[[test.results]]
value = "[{ id: pts:1, upper: 'A' }, { id: pts:2, upper: 'B' }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:3 }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:3 }]"

[[test.results]]
value = "[{ id: pts:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/
DEFINE TABLE pts SCHEMAFULL;
DEFINE FIELD point ON pts TYPE array<float>;
DEFINE FIELD tag ON pts TYPE string;
DEFINE FIELD upper ON pts COMPUTED string::uppercase(tag);
DEFINE TABLE empty SCHEMAFULL;
DEFINE FIELD point ON empty TYPE array<float>;
CREATE pts:1 SET point = [1, 1], tag = 'a';
CREATE pts:2 SET point = [2, 2], tag = 'b';
CREATE pts:3 SET point = [3, 3], tag = 'a';
CREATE pts:4 SET point = [10, 10], tag = 'b';
SELECT id, upper FROM pts WHERE point <|2,EUCLIDEAN|> array::map([1, 1], |$x| <float> $x) ORDER BY id;
SELECT id FROM pts WHERE upper = 'A' AND point <|2,EUCLIDEAN|> array::map([1, 1], |$x| <float> $x) ORDER BY id;
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> array::map([1, 1], |$x| <float> $x) AND upper = 'A' ORDER BY id;
SELECT id FROM pts WHERE tag = 'b' AND point <|1,EUCLIDEAN|> array::map([1, 1], |$x| <float> $x);
SELECT id FROM empty WHERE point <|2,EUCLIDEAN|> array::map([1, 1], |$x| <float> $x);
SELECT id FROM empty WHERE point <|2,EUCLIDEAN|> array::map(['a'], |$x| $x);
