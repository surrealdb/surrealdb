/**
[env]
namespace = true
database = true

[test]
reason = """
A stored function whose body reads an argument where only a value answers (a
MATCHES operand) is compiled for each call's argument values, and the compiled
body is kept for later calls with the same values. Values that compare equal
but differ in type are not the same values: `1`, `1.0f` and `1dec` are equal
numbers, as are `0.0f` and `-0.0f`, and arrays of them are equal arrays, yet a
body compiled with one folded in answers `type::of` and `<string>` for that
one. Each call answers for its own argument, whichever calls came before it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, txt: '1' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['int', '1', [t:1]]"

[[test.results]]
value = "['float', '1f', []]"

[[test.results]]
value = "['decimal', '1', []]"

[[test.results]]
value = "['float', '0f', []]"

[[test.results]]
value = "['float', '-0f', []]"

[[test.results]]
value = "['array', '[1]', []]"

[[test.results]]
value = "['array', '[1f]', []]"

[[test.results]]
value = "['int', '1', [t:1]]"

*/

DEFINE ANALYZER words TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE t;
DEFINE INDEX ft ON t FIELDS txt FULLTEXT ANALYZER words BM25;
CREATE t:1 SET txt = '1';
DEFINE FUNCTION fn::f($q: any) {
	RETURN [type::of($q), <string> $q, (SELECT VALUE id FROM t WHERE txt @@ $q)]
};

RETURN fn::f(1);
RETURN fn::f(1.0f);
RETURN fn::f(1dec);
RETURN fn::f(0.0f);
RETURN fn::f(-0.0f);
RETURN fn::f([1]);
RETURN fn::f([1.0f]);
RETURN fn::f(1);
