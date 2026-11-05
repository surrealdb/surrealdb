/**
[env]
namespace = true
database = true

[test]
reason = "A closure in a field clause reads the record it is evaluated for, calls a user-defined function, nests another closure, folds, and is held in a LET of a block clause; a field PERMISSIONS clause holds one too, and every write to the table runs the clauses."

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
value = "[{ id: g:1, k: ['a-x', 'b-x'], m: ['P!'], q: ['ok'], r: [2, 4], s: 6, suffix: '-x' }]"

[[test.results]]
value = "[{ id: g:1, k: ['a-x-x', 'b-x-x'], m: ['P!!'], q: ['bad'], r: [4, 8], s: 10, suffix: '-x' }]"

[[test.results]]
value = "[{ id: g:1, k: ['a-x-x', 'b-x-x'], m: ['P!!'], q: ['bad'], r: [4, 8], s: 10, suffix: '-x' }]"

[[test.results]]
value = "[{ id: g:1, k: ['zz-x'], m: ['P!!!'], q: ['bad'], r: [8, 16], s: 4, suffix: '-x' }]"

[[test.results]]
value = "[{ id: g:1, k: ['zz-x'], m: ['P!!!'], q: ['bad'], r: [8, 16], s: 4, suffix: '-x' }]"

*/
DEFINE FUNCTION fn::up($s: string) { RETURN string::uppercase($s) };
DEFINE FIELD k ON g VALUE $value.map(|$t| $t + <string>$this.suffix);
DEFINE FIELD m ON g VALUE $value.map(|$t| fn::up($t)).map(|$u| [$u].map(|$w| $w + '!')[0]);
DEFINE FIELD q ON g TYPE option<array> PERMISSIONS FOR create, update WHERE $value.all(|$x| $x != 'bad') FOR select FULL;
DEFINE FIELD r ON g VALUE { LET $f = |$a| $a * 2; RETURN $value.map($f) };
DEFINE FIELD s ON g COMPUTED k.map(|$x| string::len($x)).fold(0, |$acc, $v| $acc + $v);
CREATE g:1 SET suffix = '-x', k = ['a', 'b'], m = ['p'], q = ['ok'], r = [1, 2];
UPDATE g:1 SET q = ['bad'];
SELECT * FROM g;
UPDATE g:1 SET k = ['zz'] RETURN AFTER;
DELETE g:1 RETURN BEFORE;
