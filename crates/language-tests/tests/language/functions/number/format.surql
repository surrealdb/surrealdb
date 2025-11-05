/**
[test]

[[test.results]]
value = "'645'"

[[test.results]]
value = "'1a5'"

[[test.results]]
value = "'1A5'"

[[test.results]]
value = "'110100101'"

[[test.results]]
value = "'9.87654321e9'"

[[test.results]]
value = "'9.87654321E9'"

[[test.results]]
error = "Incorrect arguments for function number::format(). The second argument must be a valid format."

[[test.results]]
value = "'645'"

[[test.results]]
value = "'1a5'"

[[test.results]]
value = "'1A5'"

[[test.results]]
value = "'110100101'"

[[test.results]]
value = "'9.87654321e9'"

[[test.results]]
value = "'9.87654321E9'"

[[test.results]]
error = "Incorrect arguments for function number::format(). The second argument must be a valid format."

[[test.results]]
value = "'2.99792458e5'"

*/

// octal
number::format(421, "o");

// hexa
number::format(421, "x");
number::format(421, "X");

// binary
number::format(421, "b");

// exp
number::format(9_876_543_210, "e");
number::format(9_876_543_210, "E");

// unrecognized patterns
number::format(421, "f");

// used as dot function
421.format("o");
421.format("x");
421.format("X");
421.format("b");
9_876_543_210.format("e");
9_876_543_210.format("E");
421.format("f");

// scientific notation for floating point
299_792.458.format("e"); // speed of light