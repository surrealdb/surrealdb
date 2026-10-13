/**
[test]
reason = "Tests array::sort_natural_lexical function form: case-insensitive ordering with embedded numbers compared numerically, asc/desc order argument"

[[test.results]]
value = "['File1', 'file2', 'File10']"

[[test.results]]
value = "['a1', 'A2', 'a10']"

[[test.results]]
value = "['x3y', 'x20y', 'x100y']"

[[test.results]]
value = "['B10', 'b2']"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2, 10]"
*/

array::sort_natural_lexical(['File10', 'file2', 'File1']);
array::sort_natural_lexical(['a10', 'A2', 'a1']);
array::sort_natural_lexical(['x100y', 'x20y', 'x3y']);
array::sort_natural_lexical(['b2', 'B10'], 'desc');
array::sort_natural_lexical(['b', 'a'], 'up');
array::sort_natural_lexical([]);
array::sort_natural_lexical([2, 10, 1]);
