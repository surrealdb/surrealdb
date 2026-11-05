/**
[test]
reason = "Tests array::sort_lexical function form: case-insensitive dictionary order for strings, asc/desc/bool order argument, non-string fallback ordering"

[[test.results]]
value = "['Apple', 'banana', 'cherry']"

[[test.results]]
value = "['a', 'b', 'c']"

[[test.results]]
value = "['item1', 'item10', 'item2']"

[[test.results]]
value = "['Ábaco', 'apple', 'Zoo']"

[[test.results]]
value = "['a', 'b', 'c']"

[[test.results]]
value = "['c', 'b', 'a']"

[[test.results]]
value = "['b', 'a']"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2, 3]"
*/

array::sort_lexical(['banana', 'Apple', 'cherry']);
array::sort_lexical(['b', 'a', 'c']);
array::sort_lexical(['item10', 'item2', 'item1']);
array::sort_lexical(['Zoo', 'Ábaco', 'apple']);
array::sort_lexical(['c', 'a', 'b'], 'asc');
array::sort_lexical(['c', 'a', 'b'], 'desc');
array::sort_lexical(['b', 'a'], false);
array::sort_lexical(['b', 'a'], 'sideways');
array::sort_lexical([]);
array::sort_lexical([3, 1, 2]);
