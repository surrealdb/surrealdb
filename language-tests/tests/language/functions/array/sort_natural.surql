/**
[test]
reason = "Tests array::sort_natural function form: embedded numbers in strings compared numerically, asc/desc/bool order argument, non-string fallback ordering"

[[test.results]]
value = "['item1', 'item2', 'item10']"

[[test.results]]
value = "['file1.txt', 'file2.txt', 'file10.txt']"

[[test.results]]
value = "['3', '20', '100']"

[[test.results]]
value = "['A', 'B', 'a', 'b']"

[[test.results]]
value = "['item10', 'item2']"

[[test.results]]
value = "['item2', 'item10']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2, 10]"
*/

array::sort_natural(['item10', 'item2', 'item1']);
array::sort_natural(['file2.txt', 'file10.txt', 'file1.txt']);
array::sort_natural(['100', '20', '3']);
array::sort_natural(['a', 'B', 'b', 'A']);
array::sort_natural(['item10', 'item2'], 'desc');
array::sort_natural(['item10', 'item2'], true);
array::sort_natural([]);
array::sort_natural([10, 2, 1]);
